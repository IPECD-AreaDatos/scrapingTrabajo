"""
VALIDATE - Módulo de validación de datos IPC
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateIPC:
    def validate(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame IPC vacío.")
        for col in ['fecha', 'id_region']:
            if col not in df.columns:
                raise ValueError(f"[VALIDATE] Columna '{col}' faltante.")
        logger.info("[VALIDATE] OK — IPC: %d filas.", len(df))
