from .extract import extract_supermercado_data
from .transform import transform_supermercado_data
from .load import load_supermercado_data
from etl_modular.utils.logger import setup_logger
from etl_modular.utils.db import ConexionBaseDatos
import sys
import os
from dotenv import load_dotenv

def run_supermercado():
    logger = setup_logger("supermercado")
    logger.info("="*80) # Línea de asteriscos para separar visualmente
    logger.info("Iniciando el proceso de ETL para SUPERMERCADO.") # <-- ¡Este mensaje SÍ SE ESCRIBIRÁ!
    logger.info("="*80)

    load_dotenv()
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')
    db = ConexionBaseDatos(host, user, password, database)
    db.connect_db()

    try: 
        logger.info("Extrayendo datos SUPERMERCADO...")
        ruta = extract_supermercado_data()
        logger.info("Extraccion de datos SUPERMERCADO completada.")
        
        logger.info("Iniciando transformacion de valores SUPERMERCADO.")
        df = transform_supermercado_data(ruta)
        logger.info("Transformacion de valores SUPERMERCADO finalizada.")

        logger.info("Iniciando carga de valores de SUPERMERCADO.") 
        datos_nuevos = load_supermercado_data(df, db)
        print(datos_nuevos)
        logger.info("Carga de valores SUPERMERCADO finalizada.")

        #if datos_nuevos:
        #    logger.info("Se insertaron nuevos datos en la tabla de SUPERMERCADO.")
        #else:
        #    logger.info("No hubo nuevos datos para insertar.")

    except Exception as e:
        logger.info(f"Error en el proceso principal: {e}")
        sys.exit(1)
    finally:
        db.close_connections()

    logger.info("Proceso SUPERMERCADO de ETL finalizado con exito.")
