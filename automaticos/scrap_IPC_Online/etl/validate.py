"""
VALIDATE - Módulo de validación de datos IPC_Online
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateIPCOnline:
    def validate(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame IPC Online vacío.")
        for col in ['fecha', 'variacion_mensual']:
            if col not in df.columns:
                raise ValueError(f"[VALIDATE] Columna '{col}' faltante.")
        logger.info("[VALIDATE] OK — IPC Online: %d filas.", len(df))
