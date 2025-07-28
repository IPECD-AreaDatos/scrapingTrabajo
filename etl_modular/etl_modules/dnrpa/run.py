import os
from dotenv import load_dotenv
from etl_modular.utils.logger import setup_logger
from etl_modular.etl_modules.dnrpa.extract import extract_dnrpa_data
from etl_modular.etl_modules.dnrpa.transform import transform_dnrpa_data
from etl_modular.etl_modules.dnrpa.load import load_dnrpa_data
from etl_modular.etl_modules.dnrpa.sheets import load_dnrpa_sheets_data
from etl_modular.utils.db import ConexionBaseDatos

def run_dnrpa(mode='last'):
    setup_logger("dnrpa")

    load_dotenv()
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    conexion_db = None
    datos_nuevos_loaded = False  # <-- para evitar referencia no definida
    df_for_sheets = None

    try:
        conexion_db = ConexionBaseDatos(host, user, password, database)
        conexion_db.connect_db()

        if mode == 'historical':
            print("\n--- INICIANDO CARGA HISTÓRICA ---")
            raw_data_historical = extract_dnrpa_data(mode='historical')
            if raw_data_historical:
                df_transformed_historical = transform_dnrpa_data(raw_data_historical)
                load_dnrpa_data(df_transformed_historical, conexion_db) 
                print("✅ Carga histórica de DNRPA finalizada.")
            else:
                print("⚠️ No se extrajeron datos históricos, omitiendo la carga histórica.")
        
        elif mode == 'last':
            print("\n--- INICIANDO CARGA DEL ÚLTIMO AÑO ---")
            raw_data_last = extract_dnrpa_data(mode='last')
            if raw_data_last:
                df_transformed_last = transform_dnrpa_data(raw_data_last)
                if not df_transformed_last.empty:
                    datos_nuevos_loaded = load_dnrpa_data(df_transformed_last, conexion_db) 
                    print("✅ Carga del último año de DNRPA finalizada.")
                    load_dnrpa_sheets_data(datos_nuevos_loaded, df_transformed_last)
                else:
                    print("⚠️ Datos transformados vacíos, omitiendo carga a DB.")
            else:
                print("⚠️ No se extrajeron datos del último año.")
        else:
            print(f"⚠️ Modo '{mode}' no reconocido. Use 'last' o 'historical'.")

    except Exception as e:
        print(f"❌ Error en el proceso ETL de DNRPA: {e}")
    finally:
        if conexion_db:
            conexion_db.close_connections()