"""
MAIN - Orquestador ETL para EMAE
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractEMAE, TransformEMAE, LoadEMAE
from etl.validate import ValidateEMAE
from utils.logger import setup_logger


def main():
    setup_logger("emae_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL EMAE - %s ===", inicio)

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
        ruta_val, ruta_var = ExtractEMAE().extract()
        df_val, df_var = TransformEMAE().transform(ruta_val, ruta_var)
        ValidateEMAE().validate(df_val, df_var)
        loader = LoadEMAE(host, user, pwd, db)
        loader.load(df_val, df_var)
        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)
    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        raise
    finally:
        if loader:
            loader.close()


if __name__ == '__main__':
    main()