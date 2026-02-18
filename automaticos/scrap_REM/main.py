"""
MAIN - Orquestador ETL para REM
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractREM, TransformREM, LoadREM
from etl.validate import ValidateREM
from utils.logger import setup_logger


def main():
    setup_logger("rem_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL REM - %s ===", inicio)

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
        ExtractREM().extract()
        df_precios, df_cambio = TransformREM().transform()
        ValidateREM().validate(df_precios, df_cambio)
        loader = LoadREM(host, user, pwd, db)
        loader.load(df_precios, df_cambio)
        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)
    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        raise
    finally:
        if loader:
            loader.close()


if __name__ == '__main__':
    main()
