import os
import sys
from dotenv import load_dotenv
from etl_modular.utils.db import ConexionBaseDatos
from etl_modular.utils.logger import setup_logger
from etl_modular.etl_modules.ipicorr.extract import extract_ipicorr
from etl_modular.etl_modules.ipicorr.transform import transform_ipicorr
from etl_modular.etl_modules.ipicorr.load import load_ipicorr

def run_ipicorr():
    logger = setup_logger("ipicorr")
    logger.info("=" * 80)
    logger.info("Iniciando el proceso de ETL para IPICORR.")
    logger.info("=" * 80)

    load_dotenv()
    host = os.getenv("HOST_DBB")
    user = os.getenv("USER_DBB")
    password = os.getenv("PASSWORD_DBB")
    db_name = os.getenv("NAME_DBB_DATALAKE_ECONOMICO")

    db = ConexionBaseDatos(host, user, password, db_name)
    db.connect_db()

    try:
        logger.info("📥 Extrayendo datos...")
        df = extract_ipicorr()
        logger.info("✅ Extracción completada.")
        
        logger.info("🔁 Transformando datos...")
        df = transform_ipicorr(df)
        logger.info("✅ Transformación completada.")
        
        logger.info("📤 Cargando datos a la base...")
        cambios = load_ipicorr(df, db)
        if cambios:
            logger.info("✅ Nuevos datos insertados.")
        else:
            logger.info("⚠️ No hubo nuevos datos para insertar.")

    except Exception as e:
        logger.error(f"❌ Error en el proceso IPICORR: {e}")
        sys.exit(1)

    finally:
        db.close_connections()
        logger.info("🔚 Proceso IPICORR finalizado.")