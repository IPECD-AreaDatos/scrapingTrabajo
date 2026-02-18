"""
VALIDATE - Módulo de validación de datos IPI
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateIPI:
    def validate(self, df_valores: pd.DataFrame, df_variaciones: pd.DataFrame,
                 df_var_inter_acum: pd.DataFrame):
        self._validar(df_valores,       "valores",       ['fecha', 'ipi_manufacturero'])
        self._validar(df_variaciones,   "variaciones",   ['fecha', 'var_IPI'])
        self._validar(df_var_inter_acum, "acum_interanual", ['fecha', 'ipi_manufacturero_inter_acum'])
        logger.info("[VALIDATE] OK — todos los DataFrames del IPI son válidos.")

    def _validar(self, df: pd.DataFrame, nombre: str, cols: list):
        if df is None or df.empty:
            raise ValueError(f"[VALIDATE] DataFrame '{nombre}' vacío.")
        faltantes = [c for c in cols if c not in df.columns]
        if faltantes:
            raise ValueError(f"[VALIDATE] '{nombre}' le faltan columnas: {faltantes}")
        logger.info("[VALIDATE] '%s' OK — %d filas", nombre, len(df))
