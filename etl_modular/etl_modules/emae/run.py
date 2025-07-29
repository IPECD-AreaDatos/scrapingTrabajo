import os 
import sys
from dotenv import load_dotenv
from etl_modular.utils.logger import setup_logger

# Archivos de ejecucion
from etl_modular.etl_modules.emae.extract import extract_emae_data
from etl_modular.etl_modules.emae.transform import transform_emae_valores, transform_emae_variaciones
from etl_modular.etl_modules.emae.load import load_emae_valores, load_emae_variaciones
from etl_modular.utils.db import ConexionBaseDatos

def run_emae():
    logger = setup_logger("emae")
    logger.info("="*80) # Línea de asteriscos para separar visualmente
    logger.info("Iniciando el proceso de ETL para EMAE.") # <-- ¡Este mensaje SÍ SE ESCRIBIRÁ!
    logger.info("="*80)

    load_dotenv()
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    dbb_datalake = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')
    db = ConexionBaseDatos(host, user, password, dbb_datalake)
    db.connect_db()

    try:
        logger.info("-" * 40) 
        logger.info("Extrayendo datos EMAE...")
        extract_emae_data()
        logger.info("Extraccion de datos EMAE completada.")
        logger.info("-" * 40)

        logger.info("-" * 40)
        logger.info("Iniciando transformacion de valores EMAE.")
        df_valores = transform_emae_valores()
        logger.info("Transformacion de valores EMAE finalizada.")
        logger.info("-" * 40)

        logger.info("-" * 40)
        logger.info("Iniciando transformacion de variaciones EMAE.") 
        df_variaciones = transform_emae_variaciones()
        logger.info("Transformacion de variaciones EMAE finalizada.")
        logger.info("-" * 40)

        logger.info("-" * 40)
        logger.info("Iniciando carga de valores de EMAE.") 
        bandera_valores = load_emae_valores(df_valores, db)
        logger.info("Carga de valores EMAE finalizada.")
        logger.info("-" * 40)

        logger.info("-" * 40)
        logger.info("Iniciando carga de variaciones EMAE.")
        bandera_variaciones = load_emae_variaciones(df_variaciones, db)
        logger.info("Carga de variaciones EMAE finalizada.")
        logger.info("-" * 40)

        if bandera_valores or bandera_variaciones:
            logger.info("Se insertaron nuevos datos en alguna tabla.")
            
        else:
            logger.info("No hubo nuevos datos para insertar.")

    except Exception as e:
        logger.info(f"Error en el proceso principal: {e}")
        sys.exit(1)
    finally:
        db.close_connections()

    logger.info("Proceso EMAE de ETL finalizado con exito.")
    logger.info("\n") # O una línea vacía al final, aunque el formato añade cosas