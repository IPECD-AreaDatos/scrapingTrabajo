import os
from dotenv import load_dotenv
from etl_modular.utils.logger import setup_logger
from .extract import extract_ripte_data, extract_latest_ripte_value
from .transform import transform_ripte_data
from .load import load_ripte_data, load_latest_ripte_value
from etl_modular.utils.db import ConexionBaseDatos

def run_ripte(mode='last'):
    setup_logger("ripte")
    load_dotenv()

    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    conexion_db = None

    try:
        conexion_db = ConexionBaseDatos(host, user, password, database)
        conexion_db.connect_db()

        if mode == 'historical':
            print("\n--- INICIANDO CARGA HISTÓRICA DE RIPTE ---")
            ruta = extract_ripte_data()
            df = transform_ripte_data(ruta)

            if df is not None and not df.empty:
                datos_cargados = load_ripte_data(df, host, user, password, database)
                print("✅ Carga histórica de RIPTE finalizada.")
            else:
                print("⚠️ Archivo vacío o falló la transformación, omitiendo carga histórica.")

        elif mode == 'last':
            print("\n--- INICIANDO CARGA DEL ÚLTIMO VALOR DE RIPTE ---")
            ultimo_valor = extract_latest_ripte_value()
            load_latest_ripte_value(ultimo_valor, host, user, password, database)
            print("✅ Proceso de carga del último dato de RIPTE finalizado.")

        else:
            print(f"⚠️ Modo '{mode}' no reconocido. Use 'last' o 'historical'.")

    except Exception as e:
        print(f"❌ Error en el proceso ETL de RIPTE: {e}")

    finally:
        if conexion_db:
            conexion_db.close_connections()
            
            
            