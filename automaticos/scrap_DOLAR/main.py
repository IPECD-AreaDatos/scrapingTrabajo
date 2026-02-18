"""
MAIN - Orquestador ETL para DOLAR
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractDOLAR, TransformDOLAR, LoadDOLAR
from etl.validate import ValidateDOLAR
from utils.logger import setup_logger


def main():
    setup_logger("dolar_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL DOLAR - %s ===", inicio)

    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    pwd  = os.getenv('PASSWORD_DBB')
    db   = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    faltantes = [k for k, v in {'HOST_DBB': host, 'USER_DBB': user,
                                 'PASSWORD_DBB': pwd, 'NAME_DBB_DATALAKE_ECONOMICO': db}.items() if not v]
    if faltantes:
        raise ValueError(f"Variables de entorno faltantes: {faltantes}")

    loader = None
    try:
        datos_raw = ExtractDOLAR().extract()
        datos     = TransformDOLAR().transform(datos_raw)
        ValidateDOLAR().validate(datos)
        loader = LoadDOLAR(host, user, pwd, db)
        loader.load(datos)
        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)
    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        raise
    finally:
        if loader:
            loader.close()


if __name__ == '__main__':
    main()