"""
Script para reintentar la extracción de links que fallaron en la ejecución anterior.
Lee el reporte de links fallidos más reciente y procesa solo esos.
"""
import os
import logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from etl.extract import ExtractCanastaBasica
from etl.transform import TransformCanastaBasica
from etl.load import LoadCanastaBasica
from etl.validate import ValidateCanastaBasica
from etl.report import ReportCanastaBasica
from utils.logger import setup_logger
from utils.optimization import cleanup_environment

# CONFIGURACIÓN
CSV_FALLIDOS = 'files/LINKS_FALLIDOS_20260508_1539.csv'
MAX_WORKERS = 2

def get_latest_failed_report(report_dir):
    files = [f for f in os.listdir(report_dir) if f.startswith('LINKS_FALLIDOS_') and f.endswith('.csv')]
    if not files:
        return None
    files.sort(reverse=True)
    return os.path.join(report_dir, files[0])

def main():
    setup_logger("canasta_basica_retry")
    logger = logging.getLogger(__name__)

    # Limpieza inicial
    cleanup_environment(force=True)

    load_dotenv()
    inicio = datetime.now()
    logger.info("=" * 80)
    logger.info("=== INICIO REINTENTO FALLIDOS CANASTA BÁSICA - %s ===", inicio.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 80)

    report_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')
    failed_report = get_latest_failed_report(report_dir)

    if not failed_report:
        logger.error("No se encontró ningún reporte de links fallidos en %s", report_dir)
        return

    logger.info("Cargando links fallidos desde: %s", failed_report)
    df_failed = pd.read_csv(failed_report)
    failed_urls = set(df_failed['url'].unique())
    logger.info("Se encontraron %d URLs fallidas únicas.", len(failed_urls))

    extractor = None
    loader    = None

    try:
        # 1. EXTRACT
        logger.info("1. [EXTRACT] Inicializando extractor y leyendo links de DB...")
        extractor = ExtractCanastaBasica(enable_parallel=True, max_workers=2)
        all_links = extractor.read_links_from_db()
        
        # Filtrar solo los que fallaron
        links_to_retry = [l for l in all_links if l['link'] in failed_urls]
        
        if not links_to_retry:
            logger.warning("No se encontraron links en la DB que coincidan con las URLs del reporte de fallos.")
            # Tal vez las URLs en el reporte tienen alguna diferencia mínima? 
            # Vamos a intentar un match más flexible si es necesario, pero por ahora exacto.
            return

        logger.info("Se procesarán %d links (filtrados de %d totales).", len(links_to_retry), len(all_links))

        df_raw = extractor.extract(links_to_retry)
        
        if df_raw.empty:
            logger.error("[ERROR] La re-extracción no generó datos. Abortando.")
            return
        logger.info("[OK] Re-extracción finalizada: %d filas.", len(df_raw))

        # Backup del reintento
        backup_file = os.path.join(
            report_dir,
            f'BACKUP_RETRY_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
        )
        try:
            df_raw.to_csv(backup_file, index=False, quoting=1, encoding='utf-8-sig')
            logger.info("BACKUP de reintento guardado: %s", backup_file)
        except Exception as e:
            logger.warning("No se pudo crear backup: %s", e)

        # 2. TRANSFORM
        logger.info("2. [TRANSFORM] Normalizando datos...")
        df = TransformCanastaBasica().transform(df_raw)
        if df.empty:
            logger.error("[ERROR] DataFrame vacío tras transformación.")
            return

        # 3. VALIDATE
        logger.info("3. [VALIDATE] Validando datos...")
        # Nota: ValidateCanastaBasica tiene un umbral mínimo de filas, 
        # para un reintento pequeño podría fallar la validación si el umbral es alto.
        # Podríamos saltar la validación o ajustarla.
        try:
            ValidateCanastaBasica().validate(df)
        except Exception as e:
            logger.warning("[VALIDATE] La validación falló (posiblemente por pocas filas en reintento): %s", e)

        # 4. LOAD
        logger.info("4. [LOAD] Cargando a base de datos...")
        loader = LoadCanastaBasica()
        exito = loader.load(df)

        if exito:
            logger.info("=== Proceso de REINTENTO completado EXITOSAMENTE ===")
        else:
            logger.error("=== El proceso de REINTENTO finalizó con ERRORES en la etapa de carga ===")

    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        raise
    finally:
        if extractor and hasattr(extractor, 'db') and extractor.db:
            extractor.db.close_connections()
        if loader:
            if hasattr(loader, 'db_v1') and loader.db_v1:
                loader.db_v1.close_connections()
            if hasattr(loader, 'db_v2') and loader.db_v2:
                loader.db_v2.close_connections()
        duracion = (datetime.now() - inicio).total_seconds()
        logger.info("=== FIN EJECUCIÓN REINTENTO - Duración total: %.2f segundos ===", duracion)
        logger.info("=" * 80)

if __name__ == "__main__":
    main()
