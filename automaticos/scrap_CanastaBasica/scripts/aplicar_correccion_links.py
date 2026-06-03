import os
import sys
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def main():
    load_dotenv()
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    correcciones_csv = os.path.join(base_dir, 'files', 'links_corregidos.csv')
    a_modificar_csv = os.path.join(base_dir, 'files', 'links_a_modificar.csv')
    
    if not os.path.exists(correcciones_csv):
        print(f"[ERROR] No se encontró el archivo de correcciones: {correcciones_csv}")
        print("Por favor, utiliza el Asistente HTML para generarlo primero.")
        return
        
    df_correcciones = pd.read_csv(correcciones_csv)
    if df_correcciones.empty:
        print("[ERROR] El archivo de correcciones está vacío.")
        return
        
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
    
    total_arreglados = 0
    
    print("Iniciando actualización de URLs en las bases de datos...")
    
    def procesar_link_db(conn, viejo, nuevo):
        if viejo == nuevo:
            return False, "sin_cambios"
            
        q_find = text("SELECT id_link_producto, activo FROM link_productos WHERE link = :link")
        res_nuevo = conn.execute(q_find, {"link": nuevo}).fetchone()
        res_viejo = conn.execute(q_find, {"link": viejo}).fetchone()
        
        if res_nuevo is None:
            if res_viejo is not None:
                id_viejo = res_viejo[0]
                conn.execute(
                    text("UPDATE link_productos SET link = :nuevo WHERE id_link_producto = :id"),
                    {"nuevo": nuevo, "id": id_viejo}
                )
                return True, "renombrado"
        else:
            id_nuevo, activo_nuevo = res_nuevo[0], res_nuevo[1]
            changed = False
            action = []
            if res_viejo is not None:
                id_viejo = res_viejo[0]
                conn.execute(
                    text("UPDATE link_productos SET activo = 0 WHERE id_link_producto = :id"),
                    {"id": id_viejo}
                )
                changed = True
                action.append("desactivado_viejo")
            if activo_nuevo != 1:
                conn.execute(
                    text("UPDATE link_productos SET activo = 1 WHERE id_link_producto = :id"),
                    {"id": id_nuevo}
                )
                changed = True
                action.append("activado_nuevo")
                
            if changed:
                return True, "+".join(action)
                
        return False, "nada"

    try:
        with engine_v1.begin() as conn1, engine_v2.begin() as conn2:
            for _, row in df_correcciones.iterrows():
                viejo = str(row['viejo_link']).strip()
                nuevo = str(row['nuevo_link']).strip()
                
                if not viejo or not nuevo or viejo == 'nan' or nuevo == 'nan':
                    continue
                    
                # Ejecutar en MySQL
                changed1, act1 = procesar_link_db(conn1, viejo, nuevo)
                # Ejecutar en Postgres
                changed2, act2 = procesar_link_db(conn2, viejo, nuevo)
                
                if changed1 or changed2:
                    total_arreglados += 1
                    print(f"[OK] {viejo.split('/')[-1][:20]}... -> {nuevo.split('/')[-1][:20]}... (DB1: {act1}, DB2: {act2})")
                    
    except Exception as e:
        print(f"[ERROR] Error fatal al actualizar las bases de datos: {e}")
        return
    finally:
        engine_v1.dispose()
        engine_v2.dispose()
        
    print(f"\n[ÉXITO] Se actualizaron {total_arreglados} URLs en ambas bases de datos.")
    
    # Ahora limpiamos el archivo original para que no vuelvan a molestar
    if os.path.exists(a_modificar_csv):
        df_mod = pd.read_csv(a_modificar_csv)
        viejos_arreglados = df_correcciones['viejo_link'].tolist()
        
        # Filtrar los que NO están en los arreglados
        df_limpio = df_mod[~df_mod['url'].isin(viejos_arreglados)]
        
        df_limpio.to_csv(a_modificar_csv, index=False)
        print(f"[LIMPIEZA] El archivo 'links_a_modificar.csv' fue limpiado. Quedan {len(df_limpio)} links por corregir.")

if __name__ == "__main__":
    main()
