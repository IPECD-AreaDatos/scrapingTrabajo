import os
from dotenv import load_dotenv
from etl_modular.utils.logger import setup_logger
from .extract import extract_anac_data
from .transform import transform_anac_data
from .load import load_anac_data
from .sheets import load_anac_sheets_data

logger = setup_logger("anac")

def run_anac():
    logger.info("📥 Iniciando proceso ANAC...")

    try:
        load_dotenv()
        host = os.getenv('HOST_DBB')
        user = os.getenv('USER_DBB')
        password = os.getenv('PASSWORD_DBB')
        database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

        logger.info("🔽 Extrayendo datos...")
        ruta = extract_anac_data()
        logger.info("✅ Extracción completada.")

        logger.info("🔁 Transformando datos...")
        df = transform_anac_data(ruta)
        logger.info("✅ Transformación completada.")

        logger.info("📤 Cargando datos a la base...")
        datos_nuevos = load_anac_data(df)
        logger.info("✅ Carga a base completada.")

        logger.info("📤 Cargando datos a Google Sheets...")
        load_anac_sheets_data(datos_nuevos, df)
        logger.info("✅ Carga a Sheets completada.")

    except Exception as e:
        logger.error(f"❌ Error en el proceso ANAC: {e}")

    finally:
        logger.info("🔚 Proceso ANAC finalizado.")