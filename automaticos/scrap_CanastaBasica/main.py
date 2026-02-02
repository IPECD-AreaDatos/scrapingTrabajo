"""
Orquestador principal para el proceso ETL de Canasta Básica
Responsabilidad: Coordinar Extract (MySQL) → Transform → Load (MySQL)
"""
import os
import pandas as pd 
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
from utils.optimization import cleanup_environment, set_parallel_mode


def main():
    """Función principal que ejecuta el proceso ETL completo"""
    # 0. Limpieza inicial de procesos huérfanos (FORZADA)
    cleanup_environment(force=True)
    
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
        extractor = ExtractCanastaBasica(enable_parallel=True, max_workers=3)
        
        # Leemos los links activos de la tabla 'link_productos'
        # IMPORTANTE: El nombre debe ser EXACTO como está en la base de datos ('La Reina', 'Carrefour', etc.)
        #links_list = extractor.read_links_from_db(supermercado_filtro='La Reina')
        links_list = extractor.read_links_from_db()
        
        if not links_list:
            logger.error("[ERROR] No se encontraron links activos en la base de datos.")
            return
        
        # Ejecutar extracción masiva
        df_raw = extractor.extract(links_list)
        
        if df_raw.empty:
            logger.error("[ERROR] La extracción no generó datos. Abortando.")
            return
            
        logger.info("[OK] Extracción finalizada. Filas obtenidas: %d", len(df_raw))

        # =================================================================
        backup_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files', f'BACKUP_RAW_{datetime.now().strftime("%Y%m%d_%H%M")}.csv')
        os.makedirs(os.path.dirname(backup_file), exist_ok=True)
        try:
            df_raw.to_csv(backup_file, index=False)
            logger.info(f" BACKUP GUARDADO: {backup_file} (Si falla algo, los datos están aquí)")
        except Exception as e:
            logger.error(f"No se pudo crear backup: {e}")
        # =================================================================
        
        # ---------------------------------------------------------
        # 2. TRANSFORM: Limpiar y ajustar columnas para SQL
        # ---------------------------------------------------------
        logger.info("2. [TRANSFORM] Normalizando datos...")
        transformer = TransformCanastaBasica()
        
        df_transformed = transformer.transform(df_raw)
        
        if df_transformed.empty:
            logger.error("[ERROR] El DataFrame quedó vacío tras la transformación.")
            return
            
        # =================================================================
        # NUEVA VALIDACIÓN INTELIGENTE (FILTRO DE PRECIOS EN 0)
        # =================================================================
        CANTIDAD_MINIMA = 2100
        
        # 1. Limpieza preventiva: Aseguramos que la columna precio sea numérica y los NaN sean 0
        # Esto evita errores si pandas leyó algún precio como texto o vacío
        df_transformed['precio_normal'] = pd.to_numeric(df_transformed['precio_normal'], errors='coerce').fillna(0)

        # 2. Contar SOLO los productos válidos (Precio > 0)
        # Creamos un dataframe temporal solo para contar, no modificamos el original todavía
        df_validos = df_transformed[df_transformed['precio_normal'] > 0]
        
        cantidad_total = len(df_transformed)   # Total extraído (ej: 2500)
        cantidad_real = len(df_validos)        # Total con precio (ej: 2100)
        diferencia_vacios = cantidad_total - cantidad_real

        logger.info(f" REPORTE DE CALIDAD:")
        logger.info(f"   - Total filas extraídas: {cantidad_total}")
        logger.info(f"   - Productos con precio 0 (Descartados para validación): {diferencia_vacios}")
        logger.info(f"   - Productos VÁLIDOS para carga: {cantidad_real}")

        # 3. Decisión basada en CANTIDAD REAL
        if cantidad_real < CANTIDAD_MINIMA:
            logger.error(f" [VALIDACIÓN FALLIDA] Cantidad insuficiente de precios válidos: {cantidad_real}.")
            logger.error(f"   Se requiere un mínimo de {CANTIDAD_MINIMA} productos con precio > 0.")
            logger.error("   SE CANCELA LA CARGA A LA BASE DE DATOS.")
            return  # <--- SE DETIENE EL PROGRAMA
        else:
            logger.info(f" [VALIDACIÓN OK] Superado el umbral de {CANTIDAD_MINIMA} productos válidos.")
            
            # OPCIONAL: Si quieres subir SOLO los que tienen precio, descomenta la siguiente línea:
            # df_transformed = df_validos.copy() 
            
        # =================================================================

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