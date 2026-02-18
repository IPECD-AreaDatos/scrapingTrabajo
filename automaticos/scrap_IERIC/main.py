"""
MAIN - Orquestador ETL para IERIC
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractIERIC, TransformIERIC, LoadIERIC
from etl.validate import ValidateIERIC
from utils.logger import setup_logger


def main():
    setup_logger("ieric_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL IERIC - %s ===", inicio)

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
        files_dir = ExtractIERIC().extract()
        df_act, df_puestos, df_sal = TransformIERIC().transform(files_dir)
        ValidateIERIC().validate(df_act, df_puestos, df_sal)
        loader = LoadIERIC(host, user, pwd, db)
        loader.load(df_act, df_puestos, df_sal)
        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)
    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        raise
    finally:
        if loader:
            loader.close()


if __name__ == '__main__':
    main()
