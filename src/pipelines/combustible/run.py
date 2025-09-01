
from .extract import extract_combustible_data
from .transform import transform_combustible_data
from .transform import suma_por_fecha
from .load import load_combustible_data
from .sheets import load_combustible_sheets_data
from etl_modular.utils.logger import setup_logger

import os 
from dotenv import load_dotenv

logger = setup_logger("combustible")

def run_combustible():
    logger.info("=" * 80)
    logger.info("Iniciando el proceso de ETL para COMBUSTIBLE.")
    logger.info("=" * 80)

    try:
        load_dotenv()
        host = os.getenv('HOST_DBB')
        user = os.getenv('USER_DBB')
        password = os.getenv('PASSWORD_DBB')
        database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

        logger.info("🔽 Extrayendo datos...")
        ruta = extract_combustible_data()
        logger.info("✅ Extracción completada.")

        logger.info("🔁 Transformando datos...")
        df = transform_combustible_data(ruta)
        suma_mensual = suma_por_fecha(ruta)
        logger.info("✅ Transformación completada.")

        logger.info("📤 Cargando datos a la base...")
        datos_nuevos = load_combustible_data(df)
        logger.info("✅ Carga a base completada.")

        logger.info("📤 Cargando datos a Google Sheets...")
        load_combustible_sheets_data(datos_nuevos, suma_mensual)
        logger.info("✅ Carga a Sheets completada.")

    except Exception as e:
        logger.error(f"❌ Error en el proceso COMBUSTIBLE: {e}")

    finally:
        logger.info("🔚 Proceso COMBUSTIBLE finalizado.")
    