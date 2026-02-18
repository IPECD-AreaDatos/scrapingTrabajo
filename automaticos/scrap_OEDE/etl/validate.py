"""
VALIDATE - Módulo de validación de datos OEDE
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

COLUMNAS_REQUERIDAS = ['fecha', 'id_provincia', 'id_categoria', 'id_subcategoria', 'valor']


class ValidateOEDE:
    def validate(self, df: pd.DataFrame):
        if df is None or df.empty:
            raise ValueError("[VALIDATE] DataFrame vacío.")
        faltantes = [c for c in COLUMNAS_REQUERIDAS if c not in df.columns]
        if faltantes:
            raise ValueError(f"[VALIDATE] Columnas faltantes: {faltantes}")
        if df['fecha'].isnull().any():
            raise ValueError("[VALIDATE] Hay fechas nulas.")
        logger.info("[VALIDATE] OK — %d filas, rango: %s → %s",
                    len(df), df['fecha'].min(), df['fecha'].max())
