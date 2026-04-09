"""
MAIN - Orquestador ETL para Salud (Embarazo) 
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl.extract import Extract
from etl.transform import Transform
from etl.load import Load
from etl.validate import Validate
from utils.logger import setup_logger

def main():
    setup_logger("salud_embarazo_etl")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL SALUD: EMBARAZO - %s ===", inicio)

    # Variables para PostgreSQL (Base Nueva)
    host = os.getenv('HOST_DBB2')
    user = os.getenv('USER_DBB2')
    pwd  = os.getenv('PASSWORD_DBB2')
    port = os.getenv('PORT_DBB2', '5432')
    db   = os.getenv('DB_NAME_SALUD')

    # Validación de variables críticas
    config = {'HOST': host, 'USER': user, 'PASS': pwd, 'DB': db}
    faltantes = [k for k, v in config.items() if not v]
    
    if faltantes:
        logger.error("Faltan variables de entorno: %s", faltantes)
        raise ValueError(f"Variables de entorno faltantes: {faltantes}")

    loader = None
    try:
        logger.info("Extrayendo datos desde Google Sheets...")
        df_raw = Extract().extract()

        exit()
        
        logger.info("Transformando datos...")
        df = Transform().transform(df_raw)
        
        logger.info("Validando calidad de datos...")
        Validate().validate(df)
        
        logger.info("Cargando datos en PostgreSQL...")
        loader = Load(host, user, pwd, db, port)
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