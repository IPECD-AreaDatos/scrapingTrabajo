"""
Script de prueba aislado para CarrefourExtractor.
Prueba login OAuth y extracción de 10-15 productos sin tocar BD.
"""
import os
import sys
import logging
import pandas as pd

# Asegurar path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from etl.extract import ExtractCanastaBasica
from extractors.carrefour_extractor import CarrefourExtractor
from utils.logger import setup_logger

def probar_carrefour(cantidad: int = 10):
    setup_logger("test_carrefour")
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("=== PRUEBA AISLADA CARREFOUR (%d PRODUCTOS) ===", cantidad)
    logger.info("=" * 60)

    extractor_db = None
    carrefour = None

    try:
        # 1. Leer links de la base
        extractor_db = ExtractCanastaBasica()
        links_list = extractor_db.read_links_from_db()

        # Filtrar solo Carrefour
        links_carrefour = [
            l for l in links_list
            if 'carrefour' in str(l.get('supermercado', l.get('nombre_super', ''))).lower()
            or 'carrefour' in str(l.get('url', l.get('link', ''))).lower()
        ]

        if not links_carrefour:
            logger.error("No se encontraron links activos de Carrefour.")
            return

        muestra = links_carrefour[:cantidad]
        urls = [item.get('url') or item.get('link') for item in muestra]

        logger.info("Procesando %d links de Carrefour...", len(urls))

        # 2. Inicializar extractor de Carrefour y extraer
        carrefour = CarrefourExtractor()
        resultados = carrefour.extract_products(urls)

        # 3. Mostrar resumen en consola
        df_res = pd.DataFrame(resultados)
        logger.info("=" * 60)
        logger.info("=== RESULTADOS OBTENIDOS ===")
        print(df_res[['nombre', 'precio_normal', 'precio_descuento', 'url']].to_string())
        logger.info("=" * 60)

        # Guardar CSV de inspección en files/
        files_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')
        os.makedirs(files_dir, exist_ok=True)
        salida = os.path.join(files_dir, "TEST_CARREFOUR_OUT.csv")
        df_res.to_csv(salida, index=False, encoding='utf-8-sig')
        logger.info("Archivo guardado en: %s", salida)

    except Exception as e:
        logger.error("Error durante la prueba: %s", e, exc_info=True)
    finally:
        if extractor_db and hasattr(extractor_db, 'db') and extractor_db.db:
            extractor_db.db.close_connections()
        if carrefour:
            carrefour.cleanup_driver()

if __name__ == "__main__":
    limite = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 10
    probar_carrefour(cantidad=limite)