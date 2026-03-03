"""
MAIN - Orquestador ETL para ANAC
Selector dinámico de Base de Datos (Por defecto v1)
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractANAC, TransformANAC, LoadANAC
from etl.validate import ValidateANAC
from utils.logger import setup_logger

def main():
    setup_logger("anac_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL ANAC - %s ===", inicio)

    # 1. DETECTAR VERSIÓN (Por defecto '1')
    version_db = os.getenv('DB_VERSION', '1')
    logger.info(f"Usando configuración para BASE VERSIÓN {version_db}")

    if version_db == "1":
        host = os.getenv('HOST_DBB1')
        user = os.getenv('USER_DBB1')
        pwd  = os.getenv('PASSWORD_DBB1')
        port = os.getenv('PORT_DBB1') # Puede ser None
    else:
        host = os.getenv('HOST_DBB2')
        user = os.getenv('USER_DBB2')
        pwd  = os.getenv('PASSWORD_DBB2')
        port = os.getenv('PORT_DBB2')

    db_name = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    loader = None
    try:
        # EXTRACT
        logger.info("1. [EXTRACT] Descargando archivo...")
        file_path = ExtractANAC().extract(archivo_historico=False)

        # TRANSFORM
        logger.info("2. [TRANSFORM] Procesando datos...")
        df = TransformANAC().transform(file_path)

        # VALIDATE
        ValidateANAC().validate(df)

        # LOAD
        logger.info(f"3. [LOAD] Conectando a {host}...")
        # El puerto se pasa solo si existe
        loader = LoadANAC(host, user, pwd, db_name, port, version=version_db)

        if not loader.hay_datos_nuevos(df):
            logger.info("=== Base de datos actualizada. Fin del proceso ===")
            return

        loader.conectar_bdd()
        loader.load_to_database(df)

        ultimo_valor, ultima_fecha = loader.obtener_ultimo_valor_corrientes()
        if ultimo_valor is not None:
            loader.load_to_sheets(ultimo_valor, ultima_fecha)

        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)

    except Exception as e:
        logger.error("[ERROR] %s", e, exc_info=True)
        raise
    finally:
        if loader:
            loader.cerrar_conexion()

if __name__ == "__main__":
    main()