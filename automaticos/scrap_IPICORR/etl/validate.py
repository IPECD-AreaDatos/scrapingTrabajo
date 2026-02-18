"""
VALIDATE - Módulo de validación de datos IPICORR
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateIPICORR:
    def validate(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame vacío.")
        if 'Fecha' not in df.columns:
            raise ValueError("[VALIDATE] Columna 'Fecha' faltante.")
        logger.info("[VALIDATE] OK — %d filas del IPICORR.", len(df))
