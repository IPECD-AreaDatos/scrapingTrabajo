"""
VALIDATE - Módulo de validación de datos SalarioSPTotal
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateSalarioSPTotal:
    def validate(self, df_sp: pd.DataFrame, df_total: pd.DataFrame):
        self._validar(df_sp,    "sector_privado", ['salario'])
        self._validar(df_total, "total",          ['salario'])
        logger.info("[VALIDATE] OK — ambos DataFrames de salarios son válidos.")

    def _validar(self, df: pd.DataFrame, nombre: str, cols: list):
        if df is None or df.empty:
            raise ValueError(f"[VALIDATE] DataFrame '{nombre}' vacío.")
        faltantes = [c for c in cols if c not in df.columns]
        if faltantes:
            raise ValueError(f"[VALIDATE] '{nombre}' le faltan columnas: {faltantes}")
        logger.info("[VALIDATE] '%s' OK — %d filas", nombre, len(df))
