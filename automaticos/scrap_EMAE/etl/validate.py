"""
VALIDATE - Módulo de validación de datos EMAE
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateEMAE:
    def validate(self, df_valores: pd.DataFrame, df_variaciones: pd.DataFrame):
        self._validar(df_valores,     "valores",     ['fecha', 'sector_productivo', 'valor'])
        self._validar(df_variaciones, "variaciones", ['fecha', 'variacion_interanual', 'variacion_mensual'])
        logger.info("[VALIDATE] OK — ambos DataFrames del EMAE son válidos.")

    def _validar(self, df: pd.DataFrame, nombre: str, cols: list):
        if df is None or df.empty:
            raise ValueError(f"[VALIDATE] DataFrame '{nombre}' vacío.")
        faltantes = [c for c in cols if c not in df.columns]
        if faltantes:
            raise ValueError(f"[VALIDATE] '{nombre}' le faltan columnas: {faltantes}")
        logger.info("[VALIDATE] '%s' OK — %d filas", nombre, len(df))
