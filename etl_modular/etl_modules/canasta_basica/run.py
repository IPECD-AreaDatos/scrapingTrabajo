from .extract import extract_products_data

from etl_modular.utils.logger import setup_logger
from etl_modular.utils.db import ConexionBaseDatos
from dotenv import load_dotenv
import os
import sys

logger = setup_logger("canasta_basica")

def run_canasta_basica():
    logger.info("=" * 80)
    logger.info("Iniciando el proceso de ETL para CANASTA BASICA.")
    logger.info("=" * 80)

    load_dotenv()
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    db = ConexionBaseDatos(host, user, password, database)
    db.connect_db()

    try: 

        links = [
            "https://www.carrefour.com.ar/harina-de-trigo-morixe-000-1-kg/p",
        ]
        supermercado = "carrefour"

        logger.info("Extrayendo datos CANASTA BASICA de Carrefour...")
        df = extract_products_data(links, supermercado)
    

    except Exception as e:
        logger.info(f"Error en el proceso principal: {e}")
        sys.exit(1)
    finally:
        db.close_connections()

    logger.info("Proceso CANASTA BASICA de ETL finalizado con exito.")
