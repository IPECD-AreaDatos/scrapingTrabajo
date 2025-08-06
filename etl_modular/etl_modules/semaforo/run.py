import os
import sys
from dotenv import load_dotenv
from etl_modular.utils.db import ConexionBaseDatos
from etl_modular.utils.logger import setup_logger
from etl_modular.etl_modules.semaforo.extract import extract_semaforo
from etl_modular.etl_modules.semaforo.transform import transform_semaforo
from etl_modular.etl_modules.semaforo.load import load_semaforo

logger = setup_logger("semaforo")

def run_semaforo():
    logger.info("=" * 80)
    logger.info("Iniciando el proceso de ETL para SEMAFORO.")
    logger.info("=" * 80)
    

    load_dotenv()
    host = os.getenv("HOST_DBB")
    user = os.getenv("USER_DBB")
    password = os.getenv("PASSWORD_DBB")
    db_name = os.getenv("NAME_DBB_DWH_ECONOMICO")

    db = ConexionBaseDatos(host, user, password, db_name)
    db.connect_db()

    try:
        logger.info("📥 Extrayendo datos de Google Sheets...")
        df_interanual, df_intermensual = extract_semaforo()
        print(df_interanual)
        print(df_intermensual)
        logger.info("✅ Extracción completada.")

        logger.info("🔁 Transformando datos...")
        df_interanual = transform_semaforo(df_interanual)
        df_intermensual = transform_semaforo(df_intermensual)
        print(df_interanual)
        print(df_intermensual)
        logger.info("✅ Transformación completada.")

        logger.info("📤 Cargando datos en la base de datos...")
        load_semaforo(df_interanual, df_intermensual, db)
        logger.info("✅ Carga completada.")

    except Exception as e:
        logger.error(f"❌ Error en el proceso ETL Semáforo: {e}")
        sys.exit(1)

    finally:
        db.close_connections()
        logger.info("🔚 Proceso Semáforo finalizado.")