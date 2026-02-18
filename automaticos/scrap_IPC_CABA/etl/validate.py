"""
VALIDATE - Módulo de validación de datos IPC CABA
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateIPCCABA:
    def validate(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame vacío.")
        if 'fecha' not in df.columns or 'var_mensual_ipc_caba' not in df.columns:
            raise ValueError("[VALIDATE] Columnas requeridas faltantes.")
        if df['fecha'].isnull().any():
            raise ValueError("[VALIDATE] Hay fechas nulas.")
        logger.info("[VALIDATE] OK — %d filas, rango: %s → %s",
                    len(df), df['fecha'].min(), df['fecha'].max())
