from src.shared.logger import setup_logger
from src.shared.db import ConexionBaseDatos
import pandas as pd

logger = setup_logger("semaforo")

def load_semaforo(df_interanual: pd.DataFrame, df_intermensual: pd.DataFrame, db: ConexionBaseDatos):
    try:
        db.replace_table("semaforo_interanual", df_interanual)
        logger.info("📊 Tabla 'semaforo_interanual' cargada correctamente.")

        db.replace_table("semaforo_intermensual", df_intermensual)
        logger.info("📊 Tabla 'semaforo_intermensual' cargada correctamente.")

    except Exception as e:
        logger.error(f"❌ Error en carga de datos: {e}")