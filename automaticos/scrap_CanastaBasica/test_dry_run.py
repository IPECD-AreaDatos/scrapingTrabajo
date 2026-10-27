"""
Script de prueba controlada (Dry Run).
Ejecuta extracción + reporte sin cargar en bases de datos.
"""
import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd

from etl.extract import ExtractCanastaBasica
from etl.transform import TransformCanastaBasica
from etl.validate import ValidateCanastaBasica
from etl.report import ReportCanastaBasica
from utils.logger import setup_logger
from utils.optimization import cleanup_environment


def test_dry_run(limite_links: int = 20):
    setup_logger("canasta_basica_test")
    logger = logging.getLogger(__name__)

    cleanup_environment(force=True)
    load_dotenv()
    inicio = datetime.now()

    logger.info("=" * 80)
    logger.info("=== INICIO PRUEBA CONTROLADA (DRY RUN - SIN CARGA) ===")
    logger.info("Límite configurado: %d links en total.", limite_links)
    logger.info("=" * 80)

    extractor = None

    try:
        # 1. EXTRACT
        logger.info("1. [EXTRACT] Leyendo links desde la base...")
        extractor = ExtractCanastaBasica(enable_parallel=True, max_workers=2)
        links_list = extractor.read_links_from_db()

        if not links_list:
            logger.error("[ERROR] No se encontraron links activos.")
            return

        # Excluir Depot
        links_sin_depot = []
        for l in links_list:
            super_name = str(l.get('supermercado', l.get('nombre_super', ''))).lower()
            url_val = str(l.get('url', '')).lower()
            if 'depot' not in super_name and 'depot' not in url_val:
                links_sin_depot.append(l)

        excluidos = len(links_list) - len(links_sin_depot)
        if excluidos > 0:
            logger.warning("[EXTRACT] Se excluyeron %d links correspondientes a Depot.", excluidos)

        # Tomar una muestra representativa balanceada por supermercado
        df_links = pd.DataFrame(links_sin_depot)
        col_super = 'nombre_super' if 'nombre_super' in df_links.columns else 'supermercado'
        
        if col_super in df_links.columns:
            # Distribuye equitativamente entre los supermercados disponibles
            n_supers = df_links[col_super].nunique()
            por_super = max(1, limite_links // n_supers)
            df_sample = df_links.groupby(col_super).head(por_super).head(limite_links)
            muestra_links = df_sample.to_dict('records')
        else:
            muestra_links = links_sin_depot[:limite_links]

        logger.info("[EXTRACT] Se procesarán %d links de muestra para la prueba.", len(muestra_links))

        df_raw = extractor.extract(muestra_links)
        if df_raw.empty:
            logger.error("[ERROR] La extracción no devolvió datos.")
            return

        logger.info("[OK] Extracción finalizada: %d filas capturadas.", len(df_raw))

        # 2. GENERAR REPORTE DE LINKS FALLIDOS
        report_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')
        reporter = ReportCanastaBasica(report_dir)
        report_file = reporter.generate_broken_links_report(df_raw)
        if report_file:
            logger.info("Reporte de links fallidos generado en: %s", report_file)

        # 3. TRANSFORM & VALIDATE
        logger.info("2. [TRANSFORM] Transformando datos...")
        df_transformed = TransformCanastaBasica().transform(df_raw)
        
        logger.info("3. [VALIDATE] Validando...")
        ValidateCanastaBasica().validate(df_transformed)

        # 4. GUARDAR CSV LOCAL (SIN TOCAR BASE DE DATOS)
        salida_test = os.path.join(report_dir, f"TEST_RESULTADO_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        df_transformed.to_csv(salida_test, index=False, quoting=1, encoding='utf-8-sig')
        logger.info("=" * 80)
        logger.info("[ÉXITO] Archivo final de prueba guardado en: %s", salida_test)
        logger.info("NO se realizó ninguna escritura ni conexión de carga a las bases de datos.")
        logger.info("=" * 80)

    except Exception as e:
        logger.error("[ERROR CRÍTICO EN TEST] %s", e, exc_info=True)
    finally:
        if extractor and hasattr(extractor, 'db') and extractor.db:
            extractor.db.close_connections()
        duracion = (datetime.now() - inicio).total_seconds()
        logger.info("=== FIN PRUEBA - Duración: %.2f segundos ===", duracion)


if __name__ == "__main__":
    # Podés pasar la cantidad por parámetro: python test_dry_run.py 50
    cantidad = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 20
    test_dry_run(limite_links=cantidad)