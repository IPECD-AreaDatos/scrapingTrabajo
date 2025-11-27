"""
MAIN - Orquestador ETL para ANAC
Responsabilidad: Coordinar Extract → Transform → Load
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

from etl import ExtractANAC, TransformANAC, LoadANAC

def configurar_logging():
    """Configura el sistema de logging con rotación para EC2"""
    log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '..', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, 'anac_scraper.log')
    
    # Rotación de logs: máximo 5MB, mantener 2 archivos
    log_handler = RotatingFileHandler(
        log_file,
        maxBytes=5*1024*1024,  # 5MB
        backupCount=2
    )
    log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    logging.basicConfig(
        level=logging.INFO,
        handlers=[
            log_handler,
            logging.StreamHandler()
        ]
    )

def main():
    """Función principal del pipeline ETL"""
    configurar_logging()
    logger = logging.getLogger(__name__)
    
    # Cargar variables de entorno
    load_dotenv()
    
    inicio_ejecucion = datetime.now()
    logger.info(f"=== INICIO EJECUCIÓN ANAC SCRAPER - {inicio_ejecucion} ===")
    
    # Variables de entorno
    host_dbb = os.getenv('HOST_DBB')
    user_dbb = os.getenv('USER_DBB')
    pass_dbb = os.getenv('PASSWORD_DBB')
    dbb_datalake = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')
    
    # Validar variables críticas
    variables_requeridas = {
        'HOST_DBB': host_dbb,
        'USER_DBB': user_dbb, 
        'PASSWORD_DBB': pass_dbb,
        'NAME_DBB_DATALAKE_ECONOMICO': dbb_datalake,
        'GOOGLE_SHEETS_API_KEY': os.getenv('GOOGLE_SHEETS_API_KEY')
    }
    
    variables_faltantes = [var for var, valor in variables_requeridas.items() if not valor]
    if variables_faltantes:
        raise ValueError(f"Variables de entorno faltantes: {', '.join(variables_faltantes)}")
    
    loader = None
    try:
        logger.info("=== Iniciando proceso ETL ANAC ===")
        
        # ========== EXTRACT ==========
        logger.info("1. [EXTRACT] Descargando archivo...")
        extractor = ExtractANAC()
        file_path_excel = extractor.extract()
        logger.info(f"[OK] Archivo extraído: {file_path_excel}")
        
        # ========== TRANSFORM ==========
        logger.info("2. [TRANSFORM] Procesando datos Excel...")
        transformer = TransformANAC()
        df = transformer.transform(file_path_excel)
        
        if df is None or df.empty:
            raise ValueError("No se pudo generar el DataFrame desde el archivo descargado.")
        
        logger.info(f"[OK] DataFrame transformado: {len(df)} filas")
        
        # ========== LOAD ==========
        logger.info("3. [LOAD] Conectando a base de datos...")
        loader = LoadANAC(host_dbb, user_dbb, pass_dbb, dbb_datalake)
        
        # Verificar si hay datos nuevos
        logger.info("4. [LOAD] Verificando si hay datos nuevos...")
        if not loader.hay_datos_nuevos(df):
            logger.info("=== No hay datos nuevos que procesar ===")
            logger.info("El archivo Excel no contiene fechas más recientes que la BD.")
            logger.info("[SKIP] Saltando actualización de Sheets (sin datos nuevos).")
            logger.info("=== Proceso completado - Sin cambios ===")
            return
        
        # Cargar datos nuevos en la base de datos
        logger.info("5. [LOAD] Cargando datos nuevos en BD...")
        loader.conectar_bdd()
        loader.load_to_database(df)
        
        # Obtener último valor para Google Sheets
        logger.info("6. [LOAD] Preparando datos para Sheets...")
        ultimo_valor, ultima_fecha = loader.obtener_ultimo_valor_corrientes()
        
        # Actualizar Google Sheets
        if ultimo_valor is not None and ultima_fecha is not None:
            logger.info("7. [LOAD] Actualizando Google Sheets...")
            loader.load_to_sheets(ultimo_valor, ultima_fecha)
        else:
            logger.warning("[ERROR] No se pudieron obtener datos para actualizar Sheets.")
        
        fin_ejecucion = datetime.now()
        duracion = fin_ejecucion - inicio_ejecucion
        logger.info(f"=== PROCESO COMPLETADO EXITOSAMENTE - Duración: {duracion} ===")
        
    except Exception as e:
        fin_ejecucion = datetime.now()
        duracion = fin_ejecucion - inicio_ejecucion
        logger.error(f"[ERROR] ERROR DURANTE EJECUCIÓN: {e} - Duración: {duracion}")
        raise
        
    finally:
        # Cerrar conexión a BD
        if loader:
            loader.cerrar_conexion()
            logger.info("Conexión a BD cerrada.")


if __name__ == "__main__":
    main()

