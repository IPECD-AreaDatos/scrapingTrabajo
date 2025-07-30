from etl_modular.utils.logger import setup_logger
from etl_modular.utils.db import ConexionBaseDatos
import pandas as pd

logger = setup_logger("ipicorr")

def load_ipicorr(df: pd.DataFrame, db: ConexionBaseDatos):
    try:
        cambios = db.load_if_newer(df, table_name="ipicorr", date_column="Fecha")
        return cambios
    except Exception as e:
        logger.error(f"❌ Error al cargar IPICORR: {e}")
        return False