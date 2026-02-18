"""
VALIDATE - Módulo de validación de datos ANAC
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateANAC:
    def validate(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame ANAC vacío.")
        logger.info("[VALIDATE] OK — ANAC: %d filas.", len(df))
