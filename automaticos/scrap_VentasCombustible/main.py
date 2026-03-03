"""
MAIN - Orquestador ETL para VentasCombustible
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractVentasCombustible, TransformVentasCombustible, LoadVentasCombustible
from etl.validate import ValidateVentasCombustible
from utils.logger import setup_logger


def main():
    setup_logger("ventas_combustible_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL VENTAS COMBUSTIBLE - %s ===", inicio)

    version_db = os.getenv('DB_VERSION')
    logger.info(f"Usando configuración para BASE VERSIÓN {version_db}")

    if version_db == "1":
        host, user, pwd, port = os.getenv('HOST_DBB1'), os.getenv('USER_DBB1'), os.getenv('PASSWORD_DBB1'), os.getenv('PORT_DBB1')
    else:
        host, user, pwd, port = os.getenv('HOST_DBB2'), os.getenv('USER_DBB2'), os.getenv('PASSWORD_DBB2'), os.getenv('PORT_DBB2')

    db   = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    faltantes = [k for k, v in {'HOST_DBB': host, 'USER_DBB': user,
                                 'PASSWORD_DBB': pwd, 'NAME_DBB_DATALAKE_ECONOMICO': db}.items() if not v]
    if faltantes:
        raise ValueError(f"Variables de entorno faltantes: {faltantes}")

    loader = None
    try:
        ruta      = ExtractVentasCombustible().extract()
        transform = TransformVentasCombustible()
        df        = transform.transform(ruta)
        ValidateVentasCombustible().validate(df)
        loader = LoadVentasCombustible(host, user, pwd, db, port, version=version_db)
        loader.load(df)
        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)
    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        raise
    finally:
        if loader:
            loader.close()


if __name__ == '__main__':
    main()
