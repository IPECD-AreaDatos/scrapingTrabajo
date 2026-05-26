"""
MAIN - Orquestador ETL para CBT/CBA
Responsabilidad: Coordinar Extract → Transform → Validate → Load
"""
import os
import sys
import logging
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractorCBT, TransformerCBTCBA, connection_db, DataValidator
from utils.logger import setup_logger


def main():
    setup_logger("cbt_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL CBT/CBA - %s ===", inicio)

    # Obtener variables de conexión para Base 1 (MySQL)
    host1 = os.getenv('HOST_DBB1')
    user1 = os.getenv('USER_DBB1')
    pwd1  = os.getenv('PASSWORD_DBB1')
    port1 = os.getenv('PORT_DBB1')
    db_lake1 = os.getenv('NAME_DBB_DATALAKE_SOCIO')

    # Obtener variables de conexión para Base 2 (PostgreSQL)
    host2 = os.getenv('HOST_DBB2')
    user2 = os.getenv('USER_DBB2')
    pwd2  = os.getenv('PASSWORD_DBB2')
    port2 = os.getenv('PORT_DBB2')
    db_lake2 = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    # Validar que ambas bases estén configuradas
    if not (all([host1, user1, pwd1, db_lake1]) and all([host2, user2, pwd2, db_lake2])):
        logger.error("Faltan variables de entorno críticas para una o ambas bases de datos. Deben estar configuradas ambas bases (v1 y v2).")
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
        cargado_v1 = False
        cargado_v2 = False

        # Cargar Base 1 (MySQL)
        try:
            logger.info("[LOAD] Conectando y cargando a Base v1 (MySQL)...")
            db_loader1 = connection_db(host1, user1, pwd1, db_lake1, port=port1, version="1")
            cargado_v1 = db_loader1.load_datalake(df)
            db_loader1.close()
        except Exception as e:
            logger.error("[LOAD ERROR] Error cargando en Base v1 (MySQL): %s", e)
            raise

        # Cargar Base 2 (PostgreSQL)
        try:
            logger.info("[LOAD] Conectando y cargando a Base v2 (PostgreSQL)...")
            db_loader2 = connection_db(host2, user2, pwd2, db_lake2, port=port2, version="2")
            cargado_v2 = db_loader2.load_datalake(df)
            db_loader2.close()
        except Exception as e:
            logger.error("[LOAD ERROR] Error cargando en Base v2 (PostgreSQL): %s", e)
            raise

        # Si se cargó información nueva en cualquiera de las bases, actualizamos la API
        if cargado_v1 or cargado_v2:
            logger.info("[LOAD] Datos nuevos cargados. Enviando correo y actualizando API...")

            df['fecha'] = pd.to_datetime(df['fecha'])
            last_row = df.tail(1).iloc[0]
            data = {
                "anio": last_row['fecha'].year,
                "mes":  last_row['fecha'].month,
                "cbt":  int(last_row['cbt_nea']),
                "cba":  int(last_row['cba_nea']),
            }
            try:
                resp = requests.post("https://ecv.corrientes.gob.ar/api/create_cbt", data=data, timeout=10)
                if resp.status_code == 200:
                    logger.info("[API] Actualizada exitosamente.")
                else:
                    logger.warning("[API] Error %d: %s", resp.status_code, resp.text)
            except Exception as e:
                logger.error("[API] Error de conexión: %s", e)
        else:
            logger.info("[LOAD] Sin datos nuevos para cargar.")

        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)

    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()