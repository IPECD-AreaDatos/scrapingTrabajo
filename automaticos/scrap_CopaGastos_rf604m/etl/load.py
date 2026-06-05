import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
import io

def load_to_db():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    PROCESSED_FILE = os.path.join(BASE_DIR, "files/processed/consolidado_gastos.csv")
    
    if not os.path.exists(PROCESSED_FILE):
        print(f"File not found: {PROCESSED_FILE}")
        return

    # Load DB credentials
    # The .env is usually 2 levels up from the automaticos/script_name folder
    # Path: /home/usuario/Escritorio/Codigos/scrapingTrabajo/.env
    # Current file: /home/usuario/Escritorio/Codigos/scrapingTrabajo/automaticos/scrap_CopaGastos_rf604m/etl/load.py
    workspace_root = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))
    env_path = os.path.join(workspace_root, ".env")
    load_dotenv(dotenv_path=env_path)
    
    host = os.getenv("HOST_DBB2")
    port = os.getenv("PORT_DBB2", "5432")
    user = os.getenv("USER_DBB2")
    password = os.getenv("PASSWORD_DBB2")
    dbname = os.getenv("NAME_DB_DATOS_TABLERO")
    
    table_name = "copa_gastos"
    print(f"Connecting to DB {dbname} on {host}...")
    
    try:
        conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, dbname=dbname
        )
        cur = conn.cursor()
        
        # 1. Truncate table
        print(f"Truncating table {table_name}...")
        cur.execute(f"TRUNCATE TABLE {table_name};")
        
        # 2. Fast load using copy_from
        print("Loading data using copy_from...")
        df = pd.read_csv(PROCESSED_FILE)
        
        # Schema for rf604m: periodo, jurisdiccion, tipo_financ, partida, estado, monto
        cols = ["periodo", "jurisdiccion", "tipo_financ", "partida", "estado", "monto"]
        df = df[cols]
        
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        
        cur.copy_from(output, table_name, sep='\t', columns=cols)
        
        conn.commit()
        print(f"Load complete. {len(df)} rows inserted into {table_name}.")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error during load: {e}")

if __name__ == "__main__":
    load_to_db()
