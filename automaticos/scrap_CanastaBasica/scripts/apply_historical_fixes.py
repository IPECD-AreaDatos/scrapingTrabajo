import os
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    load_dotenv()
    
    def get_url(prefix):
        host = os.getenv(f'HOST_{prefix}')
        user = os.getenv(f'USER_{prefix}')
        pw = os.getenv(f'PASSWORD_{prefix}')
        db_env = 'NAME_DB_CANASTA' if prefix == 'DBB2' else 'NAME_DB_CANASTA_V1'
        db_default = 'canasta_basica_super' if prefix == 'DBB2' else 'canasta_basica_supermercados'
        db = os.getenv(db_env, db_default)
        port = os.getenv(f'PORT_{prefix}', '5432' if prefix == 'DBB2' else '3306')
        if prefix == 'DBB2':
            return f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"
        else:
            return f"mysql+pymysql://{user}:{pw}@{host}:{port}/{db}"

    engine_v1 = create_engine(get_url('DBB1'))
    engine_v2 = create_engine(get_url('DBB2'))
    
    # PASO 1: Corregir 2026-06-01 a 2026-05-31
    print("=== PASO 1: Renombrando 2026-06-01 a 2026-05-31 ===")
    try:
        with engine_v1.begin() as conn:
            res = conn.execute(text("UPDATE precios_productos SET fecha_extraccion = '2026-05-31' WHERE fecha_extraccion = '2026-06-01'"))
            print(f"MySQL actualizados: {res.rowcount}")
        with engine_v2.begin() as conn:
            res = conn.execute(text("UPDATE precios_productos SET fecha_extraccion = '2026-05-31' WHERE fecha_extraccion = '2026-06-01'"))
            print(f"Postgres actualizados: {res.rowcount}")
    except Exception as e:
        print(f"Error en PASO 1: {e}")

    # Función genérica de interpolación
    def interpolate(date_start, date_end, missing_dates):
        print(f"\n=== INTERPOLANDO: {date_start} -> {date_end} (Llenando: {missing_dates}) ===")
        # 1. Limpiar datos previos en missing_dates
        for m_date in missing_dates:
            print(f"Limpiando basura en {m_date}...")
            with engine_v1.begin() as conn:
                conn.execute(text(f"DELETE FROM precios_productos WHERE fecha_extraccion = '{m_date}'"))
            with engine_v2.begin() as conn:
                conn.execute(text(f"DELETE FROM precios_productos WHERE fecha_extraccion = '{m_date}'"))

        # 2. Obtener datos válidos
        q = text("SELECT id_link_producto, precio_normal, precio_descuento, precio_por_unidad, peso, nombre_producto, unidad_medida FROM precios_productos WHERE fecha_extraccion = :fecha")
        with engine_v2.connect() as conn:
            df_start = pd.read_sql(q, conn, params={"fecha": date_start}).drop_duplicates('id_link_producto')
            df_end = pd.read_sql(q, conn, params={"fecha": date_end}).drop_duplicates('id_link_producto')
        
        merged = pd.merge(df_start, df_end, on='id_link_producto', suffixes=('_start', '_end'))
        print(f"Productos coincidentes: {len(merged)}")
        if len(merged) == 0:
            print("No hay productos coincidentes para interpolar.")
            return

        total_days = len(missing_dates) + 1
        for i, m_date in enumerate(missing_dates):
            day_num = i + 1 
            print(f"Generando interpolación para {m_date}...")
            
            # Crear nueva extraccion
            with engine_v1.begin() as conn:
                conn.execute(text("INSERT INTO extracciones (fecha_inicio, fecha_fin, estado, productos_extraidos, productos_exitosos, productos_fallidos, duracion_segundos, created_at) VALUES (NOW(), NOW(), 'completada', :total, :total, 0, 0, NOW())"), {"total": len(merged)})
                res = conn.execute(text("SELECT LAST_INSERT_ID()"))
                id_ext = res.scalar()
            
            with engine_v2.begin() as conn:
                conn.execute(text("INSERT INTO extracciones (id_extraccion, fecha_inicio, fecha_fin, estado, productos_extraidos, productos_exitosos, productos_fallidos, duracion_segundos, created_at) VALUES (:id, NOW(), NOW(), 'completada', :total, :total, 0, 0, NOW())"), {"id": id_ext, "total": len(merged)})

            df_i = merged.copy()
            for col in ['precio_normal', 'precio_descuento', 'precio_por_unidad']:
                df_i[col] = (df_i[f'{col}_start'] + (df_i[f'{col}_end'] - df_i[f'{col}_start']) * (day_num / total_days)).round(2)
            
            df_final = pd.DataFrame()
            df_final['id_link_producto'] = df_i['id_link_producto']
            df_final['id_extraccion'] = id_ext
            df_final['nombre_producto'] = df_i['nombre_producto_start']
            df_final['precio_normal'] = df_i['precio_normal']
            df_final['precio_descuento'] = df_i['precio_descuento']
            df_final['precio_por_unidad'] = df_i['precio_por_unidad']
            df_final['unidad_medida'] = df_i['unidad_medida_start']
            df_final['peso'] = df_i['peso_start']
            df_final['fecha_extraccion'] = datetime.strptime(m_date, '%Y-%m-%d').date()
            df_final['created_at'] = datetime.now()
            
            print(f" - Insertando {len(df_final)} filas (ID Extraccion: {id_ext})...")
            batch_size = 500
            for start in range(0, len(df_final), batch_size):
                batch = df_final.iloc[start:start+batch_size]
                with engine_v1.begin() as conn:
                    batch.to_sql('precios_productos', conn, if_exists='append', index=False)
                with engine_v2.begin() as conn:
                    batch.to_sql('precios_productos', conn, if_exists='append', index=False)

    try:
        # PASO 2: Interpolar el 30/05
        interpolate('2026-05-29', '2026-05-31', ['2026-05-30'])
        
        # PASO 3: Interpolar el 25/05 (reemplaza los 535 registros)
        interpolate('2026-05-24', '2026-05-26', ['2026-05-25'])
        
        # PASO 4: Interpolar el 23/05
        interpolate('2026-05-22', '2026-05-24', ['2026-05-23'])
        
        print("\n=== REPARACIÓN HISTÓRICA COMPLETADA ===")
    except Exception as e:
        print(f"Error en interpolación: {e}")
    finally:
        engine_v1.dispose()
        engine_v2.dispose()

if __name__ == "__main__":
    main()
