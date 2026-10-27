"""
MAIN - Orquestador ETL para CanastaBasica
Responsabilidad: Coordinar Extract (DB) → Transform → Validate → Load (DB)
"""
import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

from etl.extract import ExtractCanastaBasica
from etl.transform import TransformCanastaBasica
from etl.load import LoadCanastaBasica
from etl.validate import ValidateCanastaBasica
from etl.report import ReportCanastaBasica
from utils.logger import setup_logger
from utils.optimization import cleanup_environment


def main():
    setup_logger("canasta_basica_scraper")
    logger = logging.getLogger(__name__)

    # Limpieza inicial de procesos huérfanos
    cleanup_environment(force=True)

    load_dotenv()
    inicio = datetime.now()
    logger.info("=" * 80)
    logger.info("=== INICIO ETL CANASTA BÁSICA - %s ===", inicio.strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("=" * 80)

    extractor = None
    loader = None

    try:
        # 1. EXTRACT
        logger.info("1. [EXTRACT] Inicializando extractor...")
        extractor = ExtractCanastaBasica(enable_parallel=True, max_workers=2)
        links_list = extractor.read_links_from_db()

        if not links_list:
            logger.error("[ERROR] No se encontraron links activos en la base de datos.")
            return

        # --- EXCLUSIÓN TEMPORAL DE DEPOT (Sitio web fuera de servicio) ---
        links_sin_depot = []
        for l in links_list:
            super_name = str(l.get('supermercado', l.get('nombre_super', ''))).lower()
            url_val = str(l.get('url', '')).lower()
            if 'depot' not in super_name and 'depot' not in url_val:
                links_sin_depot.append(l)

        excluidos = len(links_list) - len(links_sin_depot)
        if excluidos > 0:
            logger.warning("[EXTRACT] Se excluyeron %d links correspondientes a Depot.", excluidos)
        links_list = links_sin_depot

        if not links_list:
            logger.error("[ERROR] No quedaron links activos tras excluir Depot.")
            return

        # MODO PRUEBA
        if "--test" in sys.argv:
            logger.info("[TEST MODE] Limitando a un subset de links para prueba rápida.")
            df_temp = pd.DataFrame(links_list)
            col_super = 'nombre_super' if 'nombre_super' in df_temp.columns else 'supermercado'
            if col_super in df_temp.columns:
                df_temp = df_temp.groupby(col_super).head(2)
            links_list = df_temp.to_dict('records')
            logger.info(f"[TEST MODE] Links a procesar ({len(links_list)}): {links_list}")

        df_raw = extractor.extract(links_list)
        if df_raw.empty:
            logger.error("[ERROR] La extracción no generó datos. Abortando.")
            return
        logger.info("[OK] Extracción finalizada: %d filas.", len(df_raw))

        # --- GENERAR REPORTE DE LINKS FALLIDOS ---
        try:
            report_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')
            reporter = ReportCanastaBasica(report_dir)
            report_file = reporter.generate_broken_links_report(df_raw)
            if report_file:
                logger.warning(f"!!! ATENCIÓN: Se detectaron links con problemas. Reporte generado en: {report_file}")
            reporter.clean_old_reports()
        except Exception as e:
            logger.error(f"Error generando reporte de links fallidos: {e}")

        # Backup RAW
        backup_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 'files',
            f'BACKUP_RAW_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
        )
        os.makedirs(os.path.dirname(backup_file), exist_ok=True)
        try:
            df_raw.to_csv(backup_file, index=False, quoting=1, encoding='utf-8-sig')
            logger.info("BACKUP guardado: %s", backup_file)
        except Exception as e:
            logger.warning("No se pudo crear backup: %s", e)

        # Limpieza de backups: conservar solo los últimos 3
        try:
            archivos_backup = sorted(
                [f for f in os.listdir(os.path.dirname(backup_file)) if f.startswith('BACKUP_RAW_')],
                reverse=True
            )
            for viejo in archivos_backup[3:]:
                os.remove(os.path.join(os.path.dirname(backup_file), viejo))
                logger.info("Backup antiguo eliminado: %s", viejo)
        except Exception as e:
            logger.warning("No se pudo limpiar backups: %s", e)

        # 2. TRANSFORM
        logger.info("2. [TRANSFORM] Normalizando datos...")
        df = TransformCanastaBasica().transform(df_raw)
        if df.empty:
            logger.error("[ERROR] DataFrame vacío tras transformación.")
            return

        # 3. VALIDATE (Sin umbral de 2000 ni relleno automático)
        logger.info("3. [VALIDATE] Validando datos...")
        ValidateCanastaBasica().validate(df)

        # SI ES MODO TEST, CORTAMOS ACÁ PARA NO TOCAR LA BASE DE DATOS
        if "--test" in sys.argv:
            logger.info("[TEST MODE] Validación superada con éxito. Finalizando sin cargar en la base de datos.")
            return

        # 4. LOAD
        logger.info("4. [LOAD] Cargando a base de datos...")
        loader = LoadCanastaBasica()
        exito = loader.load(df)

        if exito:
            logger.info("=== Proceso ETL completado EXITOSAMENTE ===")
        else:
            logger.error("=== El proceso ETL finalizó con ERRORES en la etapa de carga ===")

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
        logger.info("=== FIN EJECUCIÓN - Duración total: %.2f segundos ===", duracion)
        logger.info("=" * 80)


if __name__ == "__main__":
    main()