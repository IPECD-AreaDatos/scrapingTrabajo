import os
from dotenv import load_dotenv
from src.shared.logger import setup_logger
from .extract import extract_mercado_central_data
from .transform import transform_mercado_central_completo
from pathlib import Path

def run_mercado_central():
    logger = setup_logger("mercado_central")

    load_dotenv()
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    logger.info("🚀 Iniciando pipeline de Mercado Central")

    try:
        # Ejecutar extracción (descarga y descompresión de archivos XLS → conversión a CSV)
        extract_mercado_central_data()
        logger.info("📥 Descarga y extracción de archivos completada")
        
        # Ejecutar transformación (lectura de archivos CSV generados)
        df_frutas, df_hortalizas = transform_mercado_central_completo()

        if df_frutas.empty:
            logger.warning("⚠️ No se encontraron registros de frutas para CTES.")
        else:
            logger.info(f"🍎 Frutas: {len(df_frutas)} registros")
            print("▶ Ejemplo frutas:\n", df_frutas.head())

        if df_hortalizas.empty:
            logger.warning("⚠️ No se encontraron registros de hortalizas para CTES.")
        else:
            logger.info(f"🥕 Hortalizas: {len(df_hortalizas)} registros")
            print("▶ Ejemplo hortalizas:\n", df_hortalizas.head())

        # TODO: Aquí podrías insertar df_frutas y df_hortalizas en sus respectivas tablas
        # insert_to_db(df_frutas, "tabla_frutas")
        # insert_to_db(df_hortalizas, "tabla_hortalizas")

        
        ruta_guardado = "/home/usuario/Escritorio/prueba"

        df_frutas.to_csv(f"{ruta_guardado}/df_frutas.csv", index=False)
        df_hortalizas.to_csv(f"{ruta_guardado}/df_hortalizas.csv", index=False)

    except Exception as e:
        logger.error(f"❌ Error durante la ejecución: {e}")
