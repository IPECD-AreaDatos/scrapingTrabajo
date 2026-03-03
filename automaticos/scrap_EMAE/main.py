"""
MAIN - Orquestador ETL para EMAE
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractEMAE, TransformEMAE, LoadEMAE, EmailEMAE
from etl.validate import ValidateEMAE
from utils.logger import setup_logger


def main():
    setup_logger("emae_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL EMAE - %s ===", inicio)

    version_db = os.getenv('DB_VERSION', '1')
    
    # Selección de variables según versión
    if version_db == "1":
        host, user, pwd, port = os.getenv('HOST_DBB1'), os.getenv('USER_DBB1'), os.getenv('PASSWORD_DBB1'), os.getenv('PORT_DBB1')
    else:
        host, user, pwd, port = os.getenv('HOST_DBB2'), os.getenv('USER_DBB2'), os.getenv('PASSWORD_DBB2'), os.getenv('PORT_DBB2')

    db = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    faltantes = [k for k, v in {'HOST_DBB': host, 'USER_DBB': user,
                                 'PASSWORD_DBB': pwd, 'NAME_DBB_DATALAKE_ECONOMICO': db}.items() if not v]
    if faltantes:
        raise ValueError(f"Variables de entorno faltantes: {faltantes}")

    loader = None
    try:
        # 1. Extraction
        ruta_val, ruta_var = ExtractEMAE().extract()
        
        # 2. Transformation
        df_val, df_var = TransformEMAE().transform(ruta_val, ruta_var)
        
        # 3. Validation
        ValidateEMAE().validate(df_val, df_var)
        
        # 4. Loading
        loader = LoadEMAE(host, user, pwd, db, port, version=version_db)
        actualizado = loader.load(df_val, df_var)
        
        # 5. Reporting (optional, if something new was loaded)
        if actualizado:
            logger.info("[MAIN] Datos nuevos detectados. Preparando envío de correo...")
            mailer = EmailEMAE(host, user, pwd, db, port, version=version_db)
            mailer.main_correo()
        else:
            logger.info("[MAIN] No hay datos nuevos para reportar.")

        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)
    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        raise
    finally:
        if loader:
            loader.close()


if __name__ == '__main__':
    main()