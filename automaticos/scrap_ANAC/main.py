"""
MAIN - Orquestador ETL para ANAC
Responsabilidad: Coordinar Extract → Transform → Validate → Load
"""
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

from etl import ExtractANAC, TransformANAC, LoadANAC
from etl.validate import ValidateANAC
from utils.logger import setup_logger


def main():
    """Función principal del pipeline ETL"""
    setup_logger("anac_scraper")
    logger = logging.getLogger(__name__)
    load_dotenv()

    inicio = datetime.now()
    logger.info("=== INICIO ETL ANAC - %s ===", inicio)

    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    pwd  = os.getenv('PASSWORD_DBB')
    db   = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    variables_requeridas = {
        'HOST_DBB': host, 'USER_DBB': user,
        'PASSWORD_DBB': pwd, 'NAME_DBB_DATALAKE_ECONOMICO': db,
        'GOOGLE_SHEETS_API_KEY': os.getenv('GOOGLE_SHEETS_API_KEY')
    }
    faltantes = [k for k, v in variables_requeridas.items() if not v]
    if faltantes:
        raise ValueError(f"Variables de entorno faltantes: {faltantes}")

    loader = None
    try:
        # EXTRACT
        logger.info("1. [EXTRACT] Descargando archivo...")
        file_path = ExtractANAC().extract()
        logger.info("[OK] Archivo extraído: %s", file_path)

        # TRANSFORM
        logger.info("2. [TRANSFORM] Procesando datos Excel...")
        df = TransformANAC().transform(file_path)

        # VALIDATE
        ValidateANAC().validate(df)
        logger.info("[OK] DataFrame transformado: %d filas", len(df))

        # LOAD
        logger.info("3. [LOAD] Conectando a base de datos...")
        loader = LoadANAC(host, user, pwd, db)

        if not loader.hay_datos_nuevos(df):
            logger.info("=== Sin datos nuevos. Proceso completado sin cambios ===")
            return

        loader.conectar_bdd()
        loader.load_to_database(df)

        ultimo_valor, ultima_fecha = loader.obtener_ultimo_valor_corrientes()
        if ultimo_valor is not None and ultima_fecha is not None:
            loader.load_to_sheets(ultimo_valor, ultima_fecha)
        else:
            logger.warning("[WARN] No se pudieron obtener datos para Sheets.")

        logger.info("=== COMPLETADO - Duración: %s ===", datetime.now() - inicio)

    except Exception as e:
        logger.error("[ERROR CRÍTICO] %s", e, exc_info=True)
        raise
    finally:
        if loader:
            loader.cerrar_conexion()


if __name__ == "__main__":
    main()
