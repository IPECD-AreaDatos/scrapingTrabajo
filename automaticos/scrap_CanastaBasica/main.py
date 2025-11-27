"""
Orquestador principal para el proceso ETL de Canasta Básica
Responsabilidad: Coordinar Extract → Transform → Load
"""
import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv

# Configurar logging
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs')
os.makedirs(log_dir, exist_ok=True)

log_file = os.path.join(log_dir, 'canasta_basica_scraper.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Importar módulos ETL
from etl import ExtractCanastaBasica, TransformCanastaBasica, LoadCanastaBasica


def main():
    """Función principal que ejecuta el proceso ETL completo"""
    inicio = datetime.now()
    logger.info("=" * 80)
    logger.info("=== INICIO EJECUCIÓN CANASTA BÁSICA SCRAPER - %s ===", inicio.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 80)
    
    extractor = None
    loader = None
    
    try:
        load_dotenv()
        
        logger.info("=== Iniciando proceso ETL Canasta Básica ===")
        
        # 1. EXTRACT: Leer datos de Sheets y extraer productos
        logger.info("1. [EXTRACT] Leyendo datos de Google Sheets...")
        extractor = ExtractCanastaBasica()
        
        all_supermarkets_data = extractor.read_links_from_sheets()
        if not all_supermarkets_data:
            logger.error("[ERROR] No se encontraron datos en Google Sheets")
            return
        
        logger.info("[OK] Se encontraron datos para %d supermercados: %s", 
                   len(all_supermarkets_data), list(all_supermarkets_data.keys()))
        
        # Mostrar estadísticas
        for supermarket, products_data in all_supermarkets_data.items():
            total_products = len(products_data)
            total_links = sum(len(product_list) for product_list in products_data.values())
            logger.info("[EXTRACT] Supermercado %s: %d productos, %d links", 
                    supermarket, total_products, total_links)
        
        # Inicializar sesiones
        logger.info("[EXTRACT] Inicializando sesiones de supermercados...")
        extractor.initialize_sessions()
        
        # Extraer productos
        logger.info("[EXTRACT] Extrayendo productos...")
        extracted_data = extractor.extract(all_supermarkets_data)
        
        if not extracted_data:
            logger.error("[ERROR] No se extrajeron datos de ningún supermercado")
            return
        
        logger.info("[OK] Extracción completada: %d supermercados procesados", len(extracted_data))
        
        # 2. TRANSFORM: Consolidar y limpiar datos
        logger.info("2. [TRANSFORM] Procesando datos...")
        transformer = TransformCanastaBasica()
        df_transformed = transformer.transform(extracted_data)
        
        if df_transformed.empty:
            logger.error("[ERROR] No hay datos transformados para cargar")
            return
        
        logger.info("[OK] DataFrame transformado: %d filas", len(df_transformed))
        
        # 3. LOAD: Guardar CSV y cargar a BD
        logger.info("3. [LOAD] Guardando datos...")
        loader = LoadCanastaBasica()
        loader.load(df_transformed, extracted_data, skip_database=True)  # Omitir carga a BD
        
        logger.info("=== Proceso ETL completado exitosamente ===")
        
    except Exception as e:
        logger.error("[ERROR] ERROR DURANTE EJECUCIÓN: %s", str(e), exc_info=True)
        raise
    
    finally:
        # Limpiar recursos
        if extractor:
            extractor.cleanup()
        if loader:
            loader.close_connections()
        
        fin = datetime.now()
        duracion = (fin - inicio).total_seconds()
        logger.info("=== FIN EJECUCIÓN - Duración: %.2f segundos ===", duracion)
        logger.info("=" * 80)


if __name__ == "__main__":
    main()

