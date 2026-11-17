"""
MAIN - Orquestador ETL para CBT/CBA
Responsabilidad: Coordinar Extract → Transform → Validate → Load
"""
import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractorCBT, TransformerCBTCBA, connection_db, DataValidator
from utils.logger import setup_logger
from etl.api_sync import sync_api


def main():
    setup_logger("cbt_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL CBT/CBA - %s ===", inicio)

    # Variables de conexión: PostgreSQL (la base MySQL anterior fue retirada)
    host = os.getenv('HOST_DBB2')
    user = os.getenv('USER_DBB2')
    pwd  = os.getenv('PASSWORD_DBB2')
    port = os.getenv('PORT_DBB2')
    db_lake = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    # Validar que la base esté configurada
    if not all([host, user, pwd, db_lake]):
        logger.error("Faltan variables de entorno críticas de la base de datos (HOST_DBB2, USER_DBB2, PASSWORD_DBB2, NAME_DBB_DATALAKE_ECONOMICO).")
        sys.exit(1)

    try:
        # EXTRACT
        logger.info("1. [EXTRACT] Descargando archivo CBT e identificando fecha INDEC...")
        ruta_cbt, fecha_indec = ExtractorCBT().descargar_archivo()

        # TRANSFORM
        logger.info("2. [TRANSFORM] Procesando datos CBT/CBA/NEA...")
        df = TransformerCBTCBA().transform_datalake(fecha_indec=fecha_indec)
        logger.info("[OK] %d registros transformados.", len(df))

        # VALIDATE
        logger.info("3. [VALIDATE] Validando datos...")
        validator = DataValidator()
        es_valido, errores, advertencias = validator.validar_dataframe(df)
        logger.info(validator.generar_reporte())
        if not es_valido:
            logger.error("[VALIDATE] Validación fallida: %s", errores)
            sys.exit(1)

        # LOAD
        logger.info("4. [LOAD] Cargando datos a los DataLakes...")
        try:
            logger.info("[LOAD] Conectando y cargando a PostgreSQL...")
            db_loader = connection_db(host, user, pwd, db_lake, port=port, version="2")
            db_loader.load_datalake(df)
            db_loader.close()
        except Exception as e:
            logger.error("[LOAD ERROR] Error cargando en PostgreSQL: %s", e)
            raise

        # La entrega tiene estado propio: también recupera meses previamente fallidos.
        logger.info("5. [API] Sincronizando ECV y EPH...")
        sync_api(df)

        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)

    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()