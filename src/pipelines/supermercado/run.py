from .extract import extract_supermercado_data
from .transform import transform_supermercado_data
from .load import load_supermercado_data
from .sheets import deflactador_supermercado_data
from .sheets import load_supermercado_deflactado_data
from .sheets import load_supermercado_sheets_data
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
    database2 = os.getenv('NAME_DBB_DWH_ECONOMICO')

    db = ConexionBaseDatos(host, user, password, database)
    db.connect_db()

    db2 = ConexionBaseDatos(host, user, password, database2)
    db2.connect_db()

    try: 
        logger.info("Extrayendo datos SUPERMERCADO...")
        ruta = extract_supermercado_data()
        logger.info("Extraccion de datos SUPERMERCADO completada.")
        
        logger.info("Iniciando transformacion de valores SUPERMERCADO.")
        df = transform_supermercado_data(ruta)
        logger.info("Transformacion de valores SUPERMERCADO finalizada.")

        logger.info("Iniciando carga de valores de SUPERMERCADO.") 
        datos_nuevos = load_supermercado_data(df, db)
        logger.info("Carga de valores SUPERMERCADO finalizada.")

        if datos_nuevos:
            logger.info("Se insertaron nuevos datos en la tabla de SUPERMERCADO.")
        else:
            logger.info("No hubo nuevos datos para insertar.")

        logger.info("Iniciando deflactacion de SUPERMERCADO.") 
        df_deflactado = deflactador_supermercado_data(df, db)
        print("deflactadoooooo")
        print(df_deflactado)
        logger.info("Deflactacion de SUPERMERCADO finalizada.")

        logger.info("Iniciando carga de valores de SUPERMERCADO deflactado.") 
        datos_nuevos2 = load_supermercado_deflactado_data(df_deflactado, db2)
        logger.info("Carga de valores SUPERMERCADO deflactado finalizada.")

        if datos_nuevos2:
            logger.info("Se insertaron nuevos datos en la tabla de SUPERMERCADO deflactado.")
        else:
            logger.info("No hubo nuevos datos para insertar.")

        logger.info("Iniciando carga de valores deflactados al sheets de SUPERMERCADO.") 
        datos_nuevos = load_supermercado_sheets_data(df_deflactado, datos_nuevos2)
        logger.info("Carga al sheets SUPERMERCADO finalizada.")

    except Exception as e:
        logger.info(f"Error en el proceso principal: {e}")
        sys.exit(1)
    finally:
        db.close_connections()

    logger.info("Proceso SUPERMERCADO de ETL finalizado con exito.")
