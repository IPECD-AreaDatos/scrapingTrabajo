"""
MAIN - Orquestador ETL para Semáforo
Responsabilidad: Coordinar Extract → Transform → Validate → Load
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractSemaforo, TransformSemaforo, LoadSemaforo
from etl.validate import ValidateSemaforo
from utils.logger import setup_logger


def main():
    setup_logger("semaforo_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL SEMÁFORO - %s ===", inicio)

    version_db = os.getenv('DB_VERSION', '1')
    
    # Selección de variables según versión
    if version_db == "1":
        host, user, pwd, port = os.getenv('HOST_DBB1'), os.getenv('USER_DBB1'), os.getenv('PASSWORD_DBB1'), os.getenv('PORT_DBB1')
    else:
        host, user, pwd, port = os.getenv('HOST_DBB2'), os.getenv('USER_DBB2'), os.getenv('PASSWORD_DBB2'), os.getenv('PORT_DBB2')

    db   = os.getenv('NAME_DBB_DWH_ECONOMICO')

    variables_faltantes = [k for k, v in {
        'HOST_DBB': host, 'USER_DBB': user,
        'PASSWORD_DBB': pwd, 'NAME_DBB_DWH_ECONOMICO': db,
        'GOOGLE_SHEETS_API_KEY': os.getenv('GOOGLE_SHEETS_API_KEY')
    }.items() if not v]
    if variables_faltantes:
        raise ValueError(f"Variables de entorno faltantes: {variables_faltantes}")

    loader = None
    try:
        # 1. EXTRACT
        logger.info("1. [EXTRACT] Leyendo datos desde Google Sheets...")
        extractor = ExtractSemaforo()
        df_interanual, df_intermensual = extractor.extract()

        # 2. TRANSFORM
        logger.info("2. [TRANSFORM] Procesando datos...")
        transformer = TransformSemaforo()
        df_inter_t, df_interm_t = transformer.transform(df_interanual, df_intermensual)

        # 3. VALIDATE
        logger.info("3. [VALIDATE] Validando datos...")
        ValidateSemaforo().validate(df_inter_t, df_interm_t)

        # 4. LOAD
        logger.info("4. [LOAD] Cargando datos en BD...")
        loader = LoadSemaforo(host, user, pwd, db, port, version=version_db)
        # Solo cargamos si el DataFrame tiene datos
        if not df_inter_t.empty or not df_interm_t.empty:
            loader.load(df_inter_t, df_interm_t)
        else:
            logger.warning("[LOAD] No hay datos nuevos para cargar en ninguna de las tablas.")
        loader.load(df_inter_t, df_interm_t)

        duracion = datetime.now() - inicio
        logger.info("=== PROCESO COMPLETADO EXITOSAMENTE - Duración: %s ===", duracion)

    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        raise
    finally:
        if loader:
            loader.close()


if __name__ == '__main__':
    main()