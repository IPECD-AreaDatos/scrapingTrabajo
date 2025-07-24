import os
from dotenv import load_dotenv
from etl_modular.utils.logger import setup_logger
from etl_modular.etl_modules.dnrpa.extract import extract_dnrpa_data
from etl_modular.etl_modules.dnrpa.transform import transform_dnrpa_data
from etl_modular.etl_modules.dnrpa.load import load_dnrpa_data
from etl_modular.utils.db import ConexionBaseDatos

def run_dnrpa(mode='last'):
    setup_logger("dnrpa")

    load_dotenv()
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    conexion_db = None
    try:
        conexion_db = ConexionBaseDatos(host, user, password, database)
        conexion_db.connect_db()

        raw_data = extract_dnrpa_data(mode=mode)
        df_transformed = transform_dnrpa_data(raw_data)
        print(df_transformed)
        #load_dnrpa_data(df_transformed, conexion_db)
    
    except Exception as e:
        print(f"❌ Error en el proceso ETL de DNRPA: {e}")
    finally:
        if conexion_db:
            conexion_db.close_connections()

