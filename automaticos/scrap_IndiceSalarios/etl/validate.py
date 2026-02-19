"""
VALIDATE - Módulo de validación de datos Índice de Salarios
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateIndiceSalarios:
    def validate(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame vacío.")
        if 'fecha' not in df.columns:
            raise ValueError("[VALIDATE] Columna 'fecha' faltante.")
        if df['fecha'].isnull().any():
            raise ValueError("[VALIDATE] Hay fechas nulas en 'fecha'.")
        logger.info("[VALIDATE] OK — %d filas, rango: %s → %s",
                    len(df), df['fecha'].min(), df['fecha'].max())
