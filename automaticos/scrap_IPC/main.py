"""
MAIN - Orquestador ETL para IPC
Responsabilidad: Coordinar Extract → Transform → Load (+ Reporte)
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

from etl import ExtractIPC, TransformIPC, LoadIPC

def configurar_logging():
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, 'ipc_scraper.log')
    
    log_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=2)
    log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    logging.basicConfig(level=logging.INFO, handlers=[log_handler, logging.StreamHandler()])

def main():
    configurar_logging()
    logger = logging.getLogger(__name__)
    load_dotenv()
    
    inicio = datetime.now()
    logger.info(f"=== INICIO ETL IPC - {inicio} ===")

    try:
        # Variables de entorno
        host = os.getenv('HOST_DBB')
        user = os.getenv('USER_DBB')
        pwd = os.getenv('PASSWORD_DBB')
        db = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

        # 1. EXTRACT
        logger.info("1. [EXTRACT] Iniciando descarga...")
        extractor = ExtractIPC(headless=True) # True para servidor
        rutas = extractor.extract()

        # 2. TRANSFORM
        logger.info("2. [TRANSFORM] Procesando Excels...")
        transformer = TransformIPC(host, user, pwd, db)
        df_final = transformer.transform(rutas)

        if df_final.empty:
            logger.warning("DataFrame vacío. Abortando.")
            return

        # 3. LOAD (Carga + Email)
        logger.info("3. [LOAD] Cargando a BD...")
        loader = LoadIPC(host, user, pwd, db)
        
        datos_nuevos = loader.load_to_db(df_final)

        if datos_nuevos:
            logger.info("4. [REPORT] Datos nuevos detectados. Enviando correo...")
            loader.enviar_reporte()
        else:
            logger.info("4. [REPORT] No hubo datos nuevos. No se envía correo.")

        duracion = datetime.now() - inicio
        logger.info(f"=== PROCESO COMPLETADO EXITOSAMENTE - Duración: {duracion} ===")

    except Exception as e:
        logger.error(f"[ERROR CRITICO] {e}", exc_info=True)
        raise

if __name__ == "__main__":
    main()