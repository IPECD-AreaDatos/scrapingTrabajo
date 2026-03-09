"""
VALIDATE - Módulo de validación de datos SalarioMVM
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateSalarioMVM:
    def validate(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame vacío.")
        if 'fecha' not in df.columns:
            raise ValueError("[VALIDATE] Columna 'fecha' faltante.")
        logger.info("[VALIDATE] OK — %d filas del SalarioMVM.", len(df))
