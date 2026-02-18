"""
MAIN - Orquestador ETL para IPC
Responsabilidad: Coordinar Extract → Transform → Validate → Load (+ Reporte)
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractIPC, TransformIPC, LoadIPC
from etl.validate import ValidateIPC
from utils.logger import setup_logger


def main():
    setup_logger("ipc_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL IPC - %s ===", inicio)

    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    pwd  = os.getenv('PASSWORD_DBB')
    db   = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    faltantes = [k for k, v in {'HOST_DBB': host, 'USER_DBB': user,
                                 'PASSWORD_DBB': pwd, 'NAME_DBB_DATALAKE_ECONOMICO': db}.items() if not v]
    if faltantes:
        raise ValueError(f"Variables de entorno faltantes: {faltantes}")

    try:
        # EXTRACT
        logger.info("1. [EXTRACT] Iniciando descarga...")
        rutas = ExtractIPC(headless=True).extract()

        # TRANSFORM
        logger.info("2. [TRANSFORM] Procesando Excels...")
        df = TransformIPC(host, user, pwd, db).transform(rutas)

        # VALIDATE
        ValidateIPC().validate(df)

        # LOAD
        logger.info("3. [LOAD] Cargando a BD...")
        loader = LoadIPC(host, user, pwd, db)
        datos_nuevos = loader.load_to_db(df)

        if datos_nuevos:
            logger.info("4. [REPORT] Datos nuevos detectados. Enviando correo...")
            loader.enviar_reporte()
        else:
            logger.info("4. [REPORT] Sin datos nuevos. No se envía correo.")

        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)

    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        raise


if __name__ == "__main__":
    main()