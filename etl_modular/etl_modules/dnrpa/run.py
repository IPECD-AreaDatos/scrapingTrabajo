import os
from dotenv import load_dotenv
from etl_modular.utils.logger import setup_logger
from etl_modular.etl_modules.dnrpa.extract import extract_dnrpa_data
from etl_modular.etl_modules.dnrpa.transform import transform_dnrpa_data
from etl_modular.etl_modules.dnrpa.load import load_dnrpa_data
from etl_modular.etl_modules.dnrpa.sheets import load_dnrpa_sheets_data
from etl_modular.utils.db import ConexionBaseDatos

def run_dnrpa(mode='last'):
    logger = setup_logger("dnrpa")
    logger.info("=" * 80)
    logger.info("Iniciando el proceso de ETL para DNRPA.")
    logger.info("=" * 80)

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
            logger.info("📥 Iniciando carga *histórica* de DNRPA...")
            raw_data_historical = extract_dnrpa_data(mode='historical')
            if raw_data_historical:
                df_transformed_historical = transform_dnrpa_data(raw_data_historical)
                load_dnrpa_data(df_transformed_historical, conexion_db) 
                logger.info("✅ Carga histórica de DNRPA finalizada.")
            else:
                logger.warning("⚠️ No se extrajeron datos históricos, omitiendo la carga histórica.")
        
        elif mode == 'last':
            logger.info("📥 Iniciando carga del *último año* de DNRPA...")
            raw_data_last = extract_dnrpa_data(mode='last')
            if raw_data_last:
                df_transformed_last = transform_dnrpa_data(raw_data_last)
                if not df_transformed_last.empty:
                    datos_nuevos_loaded = load_dnrpa_data(df_transformed_last, conexion_db) 
                    logger.info("✅ Carga del último año de DNRPA finalizada.")
                    load_dnrpa_sheets_data(datos_nuevos_loaded, df_transformed_last)
                else:
                    logger.warning("⚠️ Datos transformados vacíos, omitiendo carga a DB.")
            else:
                logger.warning("⚠️ No se extrajeron datos del último año.")
        else:
            logger.warning(f"⚠️ Modo '{mode}' no reconocido. Use 'last' o 'historical'.")

    except Exception as e:
        logger.error(f"❌ Error en el proceso ETL de DNRPA: {e}")
    finally:
        if conexion_db:
            conexion_db.close_connections()
            logger.info("🔌 Conexión a base cerrada.")