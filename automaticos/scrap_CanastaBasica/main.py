"""
Orquestador principal para el proceso ETL de Canasta Básica
Responsabilidad: Coordinar Extract (MySQL) → Transform → Load (MySQL)
"""
import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv

# Configurar logging
log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'logs')
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
from etl.extract import ExtractCanastaBasica
from etl.transform import TransformCanastaBasica
from etl.load import LoadCanastaBasica


def main():
    """Función principal que ejecuta el proceso ETL completo"""
    inicio = datetime.now()
    logger.info("=" * 80)
    logger.info("=== INICIO EJECUCIÓN CANASTA BÁSICA SCRAPER (DB MODE) - %s ===", inicio.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 80)
    
    extractor = None
    loader = None
    
    try:
        load_dotenv()
        
        logger.info("=== Iniciando proceso ETL ===")
        
        # ---------------------------------------------------------
        # 1. EXTRACT: Leer links de la DB y extraer precios
        # ---------------------------------------------------------
        logger.info("1. [EXTRACT] Inicializando extractor...")
        extractor = ExtractCanastaBasica(enable_parallel=True, max_workers=5)
        
        # Leemos los links activos de la tabla 'link_productos'
        # IMPORTANTE: El nombre debe ser EXACTO como está en la base de datos ('La Reina', 'Carrefour', etc.)
        links_list = extractor.read_links_from_db(supermercado_filtro='La Reina')
        
        if not links_list:
            logger.error("[ERROR] No se encontraron links activos en la base de datos.")
            return
        
        # Ejecutar extracción masiva
        df_raw = extractor.extract(links_list)
        
        if df_raw.empty:
            logger.error("[ERROR] La extracción no generó datos. Abortando.")
            return
            
        logger.info("[OK] Extracción finalizada. Filas obtenidas: %d", len(df_raw))
        
        # ---------------------------------------------------------
        # 2. TRANSFORM: Limpiar y ajustar columnas para SQL
        # ---------------------------------------------------------
        logger.info("2. [TRANSFORM] Normalizando datos...")
        transformer = TransformCanastaBasica()
        
        df_transformed = transformer.transform(df_raw)
        
        if df_transformed.empty:
            logger.error("[ERROR] El DataFrame quedó vacío tras la transformación.")
            return
            
        logger.info("[OK] Transformación completada. Datos listos para carga: %d", len(df_transformed))
        
        # ---------------------------------------------------------
        # 3. LOAD: Insertar en 'precios_productos' y registrar extracción
        # ---------------------------------------------------------
        logger.info("3. [LOAD] Iniciando carga a base de datos...")
        loader = LoadCanastaBasica()
        
        exito = loader.load(df_transformed)
        
        if exito:
            logger.info("=== Proceso ETL completado EXITOSAMENTE ===")
        else:
            logger.error("=== El proceso ETL finalizó con ERRORES en la etapa de carga ===")
        
    except Exception as e:
        logger.error("[ERROR] FALLO CRÍTICO DURANTE EJECUCIÓN: %s", str(e), exc_info=True)
        raise
    
    finally:
        # Limpiar recursos
        if extractor and hasattr(extractor, 'cleanup'):
            extractor.cleanup()
        
        # Si usas conexiones persistentes en loader, cerrarlas
        if loader and hasattr(loader, 'db'):
            loader.db.close_connections()
        
        fin = datetime.now()
        duracion = (fin - inicio).total_seconds()
        logger.info("=== FIN EJECUCIÓN - Duración total: %.2f segundos ===", duracion)
        logger.info("=" * 80)


if __name__ == "__main__":
    main()