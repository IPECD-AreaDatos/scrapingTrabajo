import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def main():
    # Cargar variables de entorno
    # Estamos en /manuales/carga_copa_ROP/main.py
    # .env está en la raíz del proyecto
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, '..', '..'))
    dotenv_path = os.path.join(project_root, '.env')
    load_dotenv(dotenv_path)

    # Configuración de base de datos
    version = os.getenv("DB_VERSION", "1")
    
    db_name = os.getenv("NAME_DB_DATOS_TABLERO")
    if db_name:
        db_name = db_name.strip()
    else:
        db_name = "datos_tablero"

    # Usar HOST_DBB2 (Postgres) para la base datos_tablero
    host = os.getenv("HOST_DBB2")
    port = os.getenv("PORT_DBB2", "5432")
    user = os.getenv("USER_DBB2")
    password = os.getenv("PASSWORD_DBB2")
    
    url = f"postgresql+psycopg2://{user}:{password}@{host.strip() if host else host}:{port.strip() if port else port}/{db_name}"
    version = "2" # Forzar versión Postgres para la lógica de la llave primaria

    table_name = "copa_reca_rop"
    
    # Leer Excel
    file_path = os.path.join(current_dir, 'files', 'reca.xlsx')
    if not os.path.exists(file_path):
        print(f"Error: No se encontró el archivo {file_path}")
        return

    print(f"Cargando datos desde {file_path}...")
    df = pd.read_excel(file_path)
    
    # Mapeo de columnas (Limpieza de problemas de codificación y espacios)
    # Originales: ['ao', 'mes', 'inmobiliario rural', 'tasas', 'marcas y seales', 'sellos', 'premios', 'ingresos brutos', 'apremios, concursos, quiebras, reg. judiciales']
    mapping = {
        df.columns[0]: 'anio',
        df.columns[1]: 'mes',
        df.columns[2]: 'inmobiliario_rural',
        df.columns[3]: 'tasas',
        df.columns[4]: 'marcas_y_senales',
        df.columns[5]: 'sellos',
        df.columns[6]: 'premios',
        df.columns[7]: 'ingresos_brutos',
        df.columns[8]: 'apremios_concursos_quiebras_reg_judiciales'
    }
    df = df.rename(columns=mapping)
    
    # Asegurar tipos y limpiar
    df['anio'] = df['anio'].astype(int)
    df['mes'] = df['mes'].astype(int)
    df = df.fillna(0)
    
    # Conectar y Cargar
    print(f"Conectando a {host} ({'MySQL' if version=='1' else 'PostgreSQL'})...")
    engine = create_engine(url)
    
    try:
        with engine.begin() as conn:
            print(f"Cargando {len(df)} filas en la tabla '{table_name}'...")
            # Usamos if_exists='replace' para crear la tabla con la estructura del dataframe
            df.to_sql(table_name, con=conn, if_exists='replace', index=False)
            
            # Agregar Llave Primaria para asegurar integridad
            print("Agregando llave primaria (anio, mes)...")
            if version == "1":
                conn.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY (anio, mes)"))
            else:
                conn.execute(text(f"ALTER TABLE {table_name} ADD CONSTRAINT {table_name}_pk PRIMARY KEY (anio, mes)"))
                
        print("Proceso ETL finalizado con éxito.")
    except Exception as e:
        print(f"Error durante el ETL: {e}")

if __name__ == "__main__":
    main()
