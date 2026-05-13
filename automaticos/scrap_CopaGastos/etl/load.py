import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv
import io

def load_to_db():
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    PROCESSED_FILE = os.path.join(BASE_DIR, "files/processed/consolidado_gastos.csv")
    
    if not os.path.exists(PROCESSED_FILE):
        print(f"File not found: {PROCESSED_FILE}")
        return

    # Load DB credentials
    env_path = os.path.join(os.path.dirname(os.path.dirname(BASE_DIR)), ".env")
    load_dotenv(dotenv_path=env_path)
    
    host = os.getenv("HOST_DBB2")
    port = os.getenv("PORT_DBB2")
    user = os.getenv("USER_DBB2")
    password = os.getenv("PASSWORD_DBB2")
    dbname = os.getenv("NAME_DB_DATOS_TABLERO")
    
    print(f"Connecting to DB {dbname} on {host}...")
    
    try:
        conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, dbname=dbname
        )
        cur = conn.cursor()
        
        # 1. Truncate table for a clean load (Standard ETL practice)
        print("Truncating table copa_gastos_nuevo...")
        cur.execute("TRUNCATE TABLE copa_gastos_nuevo;")
        
        # 2. Fast load using copy_from
        print("Loading data using copy_from...")
        # We read the CSV back to a buffer for psycopg2
        df = pd.read_csv(PROCESSED_FILE)
        
        # Ensure column order matches DB schema
        # DB: mes, anio, jurisdiccion, programa, sub_prof, py, a_obra, partid, sub_partid, tipo_de_g, val
        cols = ["mes", "anio", "jurisdiccion", "programa", "sub_prof", "py", "a_obra", "partid", "sub_partid", "tipo_de_g", "val"]
        df = df[cols]
        
        output = io.StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        
        cur.copy_from(output, 'copa_gastos_nuevo', sep='\t', columns=cols)
        
        conn.commit()
        print(f"Load complete. {len(df)} rows inserted.")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Error during load: {e}")

if __name__ == "__main__":
    load_to_db()
