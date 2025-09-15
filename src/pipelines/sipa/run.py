from src.pipelines.sipa.extract import extract_sipa_data
from src.pipelines.sipa.transform import transform_sipa_data
from src.pipelines.sipa.load import load_sipa_data
from src.pipelines.sipa.send_mail import MailSipa
from src.shared.logger import setup_logger
from src.shared.db import ConexionBaseDatos
import sys

import os
from dotenv import load_dotenv

def run_sipa():
    logger = setup_logger("sipa")
    logger.info("="*80) # Línea de asteriscos para separar visualmente
    logger.info("Iniciando el proceso de ETL para SIPA.") # <-- ¡Este mensaje SÍ SE ESCRIBIRÁ!
    logger.info("="*80)

    load_dotenv()
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')
    db = ConexionBaseDatos(host, user, password, database)
    db.connect_db()

    try: 
        logger.info("Extrayendo datos SIPA...")
        ruta = extract_sipa_data()
        logger.info("Extraccion de datos SIPA completada.")

        logger.info("Iniciando transformacion de valores SIPA.")
        df = transform_sipa_data(ruta)
        logger.info("Transformacion de valores SIPA finalizada.")

        logger.info("Iniciando carga de valores de SIPA.") 
        datos_nuevos = load_sipa_data(df, db)
        logger.info("Carga de valores SIPA finalizada.")

        if datos_nuevos:
            logger.info("Se insertaron nuevos datos en la tabla de SIPA.")
        else:
            logger.info("No hubo nuevos datos para insertar.")

        if datos_nuevos:
            logger.info("Iniciando el envio de correo de valores de SIPA.") 
            mailer = MailSipa(
                host,
                user,
                password,
                database
            )
            mailer.send_mail()
            logger.info("Correo enviado!") 
        else:
            logger.info("No se detectaron datos nuevos. No se enviará correo.")
    
    except Exception as e:
        logger.info(f"Error en el proceso principal: {e}")
        sys.exit(1)
    finally:
        db.close_connections()

    logger.info("Proceso SIPA de ETL finalizado con exito.")
