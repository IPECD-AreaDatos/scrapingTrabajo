"""
MAIN - Orquestador ETL para IPI
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractIPI, TransformIPI, LoadIPI
from etl.validate import ValidateIPI
from utils.logger import setup_logger


def main():
    setup_logger("ipi_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL IPI - %s ===", inicio)

    version_db = os.getenv('DB_VERSION', '1')
    
    # Selección de variables según versión
    if version_db == "1":
        host, user, pwd, port = os.getenv('HOST_DBB1'), os.getenv('USER_DBB1'), os.getenv('PASSWORD_DBB1'), os.getenv('PORT_DBB1')
    else:
        host, user, pwd, port = os.getenv('HOST_DBB2'), os.getenv('USER_DBB2'), os.getenv('PASSWORD_DBB2'), os.getenv('PORT_DBB2')

    db_datalake_economico   = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')
    db_dwh_economico   = os.getenv('NAME_DBB_DWH_ECONOMICO')


    faltantes = [k for k, v in {'HOST_DBB': host, 'USER_DBB': user,
                                 'PASSWORD_DBB': pwd, 'NAME_DBB_DATALAKE_ECONOMICO': db_datalake_economico}.items() if not v]
    if faltantes:
        raise ValueError(f"Variables de entorno faltantes: {faltantes}")

    loader = None
    try:
        ruta = ExtractIPI().extract()
        # Transform ahora retorna un diccionario con {'valores': df, 'variaciones': df, 'acumulado': df}
        datos_ipi = TransformIPI().transform(ruta)
        
        # Pasamos el diccionario al validador
        ValidateIPI().validate(datos_ipi)
        
        loader = LoadIPI(host, user, pwd, db_datalake_economico, db_dwh_economico, port, version=version_db)
        loader.load(datos_ipi)
        
        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)
    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        raise
    finally:
        if loader:
            loader.close()


if __name__ == '__main__':
    main()
