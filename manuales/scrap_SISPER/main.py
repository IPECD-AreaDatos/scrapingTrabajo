import os
import logging
from dotenv import load_dotenv
from etl.extract import extract_sisper_range
from etl.load import CargadorSelector, load_to_dest

# Configuración de logs
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_etl():
    # 1. Cargar configuración de entorno (Busca el .env de la raíz)
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', '.env'))
    
    # Parámetros de destino
    version = os.getenv('DB_VERSION', '1') # v1 por defecto (Base Vieja)
    host_dest = os.getenv('HOST_DBB1') if version == '1' else os.getenv('HOST_DBB2')
    user_dest = os.getenv('USER_DBB1') if version == '1' else os.getenv('USER_DBB2')
    pass_dest = os.getenv('PASSWORD_DBB1') if version == '1' else os.getenv('PASSWORD_DBB2')
    db_dest = 'conexiones_externas'
    tabla_destino = 'sisper'
    
    loader = None
    try:
        # 2. Conectar al destino
        loader = CargadorSelector(host_dest, user_dest, pass_dest, db_dest, version=version)
        loader.conectar()
        
        # 3. Iniciar Extracción and Carga por chunks
        logger.info("=== INICIANDO ETL MODULAR DE SISPER (Optimizado) ===")
        extractor = extract_sisper_range(tanda_size=10000)
        
        total_filas = 0
        primera_tanda = True
        
        for df_chunk in extractor:
            # 4. Cargar tanda
            load_to_dest(df_chunk, loader, tabla_destino, primera_tanda=primera_tanda)
            
            # Contadores
            total_filas += len(df_chunk)
            primera_tanda = False
            logger.info(f"==> Total procesado hasta el momento: {total_filas} registros.")
            
        logger.info(f"=== ETL COMPLETADO: {total_filas} registros en total ===")
        
    except Exception as e:
        logger.error(f"Error crítico en el ETL de SISPER: {e}")
    finally:
        if loader:
            loader.cerrar()

if __name__ == "__main__":
    run_etl()
