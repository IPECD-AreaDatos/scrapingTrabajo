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
    
    try:
        with engine_v1.begin() as conn1, engine_v2.begin() as conn2:
            query = text("UPDATE link_productos SET link = :nuevo WHERE link = :viejo")
            
            for _, row in df_correcciones.iterrows():
                viejo = str(row['viejo_link']).strip()
                nuevo = str(row['nuevo_link']).strip()
                
                if not viejo or not nuevo or viejo == 'nan' or nuevo == 'nan':
                    continue
                    
                # Ejecutar en MySQL
                res1 = conn1.execute(query, {"nuevo": nuevo, "viejo": viejo})
                # Ejecutar en Postgres
                res2 = conn2.execute(query, {"nuevo": nuevo, "viejo": viejo})
                
                if res1.rowcount > 0 or res2.rowcount > 0:
                    total_arreglados += 1
                    print(f"[OK] {viejo.split('/')[-1][:20]}... -> {nuevo.split('/')[-1][:20]}...")
                    
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
