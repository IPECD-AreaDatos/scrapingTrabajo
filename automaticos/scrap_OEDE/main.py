"""
MAIN - Orquestador ETL para OEDE
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractOEDE, TransformOEDE, LoadOEDE
from etl.validate import ValidateOEDE
from utils.logger import setup_logger


def main():
    setup_logger("oede_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL OEDE - %s ===", inicio)

    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    pwd  = os.getenv('PASSWORD_DBB')
    db   = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    faltantes = [k for k, v in {'HOST_DBB': host, 'USER_DBB': user,
                                 'PASSWORD_DBB': pwd, 'NAME_DBB_DATALAKE_ECONOMICO': db}.items() if not v]
    if faltantes:
        raise ValueError(f"Variables de entorno faltantes: {faltantes}")

    transformer = None
    loader = None
    try:
        ruta = ExtractOEDE().extract()

        # TransformOEDE necesita conexión a BD para leer el diccionario
        transformer = TransformOEDE(host, user, pwd, db)
        df = transformer.transform(ruta)

        ValidateOEDE().validate(df)

        loader = LoadOEDE(host, user, pwd, db)
        loader.load(df)

        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)
    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        raise
    finally:
        if transformer:
            transformer.close()
        if loader:
            loader.close()


if __name__ == '__main__':
    main()
