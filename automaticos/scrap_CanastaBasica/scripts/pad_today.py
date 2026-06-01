import os
import sys
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Agregar directorio raíz del proyecto para imports
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

    engine_v2 = create_engine(get_url('DBB2'))
    
    # Fechas dinámicas
    now = datetime.now()
    date_today = now.strftime('%Y-%m-%d')
    target_count = 2000
    
    print(f"=== RELLENANDO HUECOS ({date_today}) DESDE EL ÚLTIMO DÍA DISPONIBLE ===")
    
    try:
        # Encontrar el archivo crudo más reciente
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        files_dir = os.path.join(base_dir, 'files')
        
        raw_files = [f for f in os.listdir(files_dir) if f.startswith('BACKUP_RAW_') and f.endswith('.csv')]
        if not raw_files:
            raise FileNotFoundError("No se encontró ningún archivo BACKUP_RAW_*.csv en la carpeta files.")
            
        raw_files.sort(reverse=True)
        csv_path = os.path.join(files_dir, raw_files[0])
        print(f"Usando archivo crudo: {raw_files[0]}")
        
        df_today_raw = pd.read_csv(csv_path)
        
        # Load the latest available data from DB prior to today
        q = text("""
            SELECT * FROM precios_productos 
            WHERE fecha_extraccion = (
                SELECT MAX(fecha_extraccion) 
                FROM precios_productos 
                WHERE fecha_extraccion < :hoy
            )
        """)
        with engine_v2.connect() as conn:
            df_yesterday = pd.read_sql(q, conn, params={"hoy": date_today})
            
        if not df_yesterday.empty:
            date_yesterday_actual = df_yesterday['fecha_extraccion'].iloc[0]
            print(f"Usando datos de respaldo de fecha: {date_yesterday_actual}")
        else:
            print("No se encontraron datos históricos para rellenar.")
        
        print(f"Total productos CSV raw: {len(df_today_raw)}")
        
        # We need to find which items were valid yesterday but are missing or invalid today.
        # But wait, df_today_raw doesn't have valid/invalid calculated yet!
        # Let's apply basic logic: if it has 'Precio Normal' or 'Precio Descuento', it's valid.
        # Actually, df_today_raw has 'precio_normal' and 'precio_descuento' columns.
        valid_hoy_mask = (df_today_raw['precio_normal'].fillna(0) > 0) | (df_today_raw['precio_descuento'].fillna(0) > 0)
        valid_hoy_ids = set(df_today_raw[valid_hoy_mask]['id_link_producto'].tolist())
        
        valid_count = len(valid_hoy_ids)
        print(f"Total productos válidos hoy (crudos): {valid_count}")
        
        df_final_raw = df_today_raw.copy()
        
        if valid_count < target_count:
            needed = target_count - valid_count
            print(f"Se necesitan {needed} productos adicionales.")

            # Filter yesterday's valid items
            df_missing = df_yesterday[~df_yesterday['id_link_producto'].isin(valid_hoy_ids)].copy()
            df_missing = df_missing[(df_missing['precio_normal'] > 0) | (df_missing['precio_descuento'] > 0)]
            
            if len(df_missing) < needed:
                print(f"¡ADVERTENCIA! Ayer solo tiene {len(df_missing)} productos disponibles para copiar.")
                needed = len(df_missing)
                
            df_pad_db = df_missing.head(needed).copy()
            
            # Convert DB format back to RAW CSV format
            df_pad_raw = pd.DataFrame()
            df_pad_raw['nombre'] = df_pad_db['nombre_producto']
            df_pad_raw['precio_normal'] = df_pad_db['precio_normal']
            df_pad_raw['precio_descuento'] = df_pad_db['precio_descuento']
            df_pad_raw['precio_por_unidad'] = df_pad_db['precio_por_unidad']
            df_pad_raw['unidad'] = df_pad_db['unidad_medida']
            df_pad_raw['descuentos'] = '[]'
            df_pad_raw['fecha'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df_pad_raw['supermercado'] = 'Relleno'
            df_pad_raw['url'] = 'Relleno'
            df_pad_raw['id_link_producto'] = df_pad_db['id_link_producto']
            df_pad_raw['peso'] = df_pad_db['peso']
            df_pad_raw['error_type'] = None
            df_pad_raw['titulo'] = None
            df_pad_raw['producto_nombre'] = None
            df_pad_raw['origen_dato'] = 'relleno'
            df_pad_raw['fecha_extraccion'] = date_today
            
            # Remove invalid counterparts from today so no duplicates
            df_final_raw = df_final_raw[~df_final_raw['id_link_producto'].isin(df_pad_raw['id_link_producto'])]
            
            df_final_raw = pd.concat([df_final_raw, df_pad_raw], ignore_index=True)
            print(f"Agregados {len(df_pad_raw)} productos de relleno.")
        else:
            print("Ya alcanzó o superó el umbral.")
            
        out_csv = f'files/BACKUP_PADDED_{date_today.replace("-","")}.csv'
        df_final_raw.to_csv(out_csv, index=False)
        print(f"Guardado en {out_csv}")
        print("\n=== RELLENO CSV COMPLETADO CON ÉXITO ===")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        engine_v2.dispose()

if __name__ == "__main__":
    main()
