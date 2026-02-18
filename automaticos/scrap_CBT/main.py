"""
MAIN - Orquestador ETL para CBT/CBA
Responsabilidad: Coordinar Extract → Transform → Validate → Load
"""
import os
import sys
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv

from extract.extractor_cbt import ExtractorCBT
from extract.extractor_pobreza import ExtractorPobreza
from transform.transformer_cbt_cba import TransformerCBTCBA
from load.database_loader import connection_db
from load.email_sender import MailCBTCBA
from validate.data_validator import DataValidator
from utils.logger import setup_logger


def main():
    setup_logger("cbt_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL CBT/CBA - %s ===", inicio)

    host     = os.getenv('HOST_DBB')
    user     = os.getenv('USER_DBB')
    pwd      = os.getenv('PASSWORD_DBB')
    db_lake  = os.getenv('NAME_DBB_DATALAKE_SOCIO')
    db_dwh   = os.getenv('NAME_DBB_DWH_SOCIO')

    faltantes = [k for k, v in {
        'HOST_DBB': host, 'USER_DBB': user, 'PASSWORD_DBB': pwd,
        'NAME_DBB_DATALAKE_SOCIO': db_lake, 'NAME_DBB_DWH_SOCIO': db_dwh
    }.items() if not v]
    if faltantes:
        raise ValueError(f"Variables de entorno faltantes: {faltantes}")

    try:
        # EXTRACT
        logger.info("1. [EXTRACT] Descargando archivos CBT y Pobreza...")
        ExtractorCBT().descargar_archivo()
        ExtractorPobreza().descargar_archivo()

        # TRANSFORM
        logger.info("2. [TRANSFORM] Procesando datos CBT/CBA/NEA...")
        df = TransformerCBTCBA().transform_datalake()
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
        logger.info("4. [LOAD] Cargando al DataLake...")
        db_loader = connection_db(host, user, pwd, db_lake)
        db_loader.connect_db()
        bandera = db_loader.load_datalake(df)

        if bandera:
            logger.info("[LOAD] Datos nuevos cargados. Enviando correo y actualizando API...")
            MailCBTCBA(host, user, pwd, db_dwh).send_mail_cbt_cba()

            last_row = df.tail(1).iloc[0]
            data = {
                "anio": last_row['Fecha'].year,
                "mes":  last_row['Fecha'].month,
                "cbt":  int(last_row['cbt_nea']),
                "cba":  int(last_row['cba_nea']),
            }
            resp = requests.post("https://ecv.corrientes.gob.ar/api/create_cbt", data=data)
            if resp.status_code == 200:
                logger.info("[API] Actualizada exitosamente.")
            else:
                logger.warning("[API] Error %d: %s", resp.status_code, resp.text)
        else:
            logger.info("[LOAD] Sin datos nuevos para cargar.")

        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)

    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
