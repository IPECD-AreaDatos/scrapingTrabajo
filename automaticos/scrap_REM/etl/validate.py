"""
VALIDATE - Módulo de validación de datos REM
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateREM:
    def validate(self, df_precios: pd.DataFrame, df_cambio: pd.DataFrame):
        self._validar(df_precios, "precios_minoristas", ['fecha', 'mediana'])
        self._validar(df_cambio,  "cambio_nominal",     ['fecha', 'cambio_nominal'])
        logger.info("[VALIDATE] OK — ambos DataFrames del REM son válidos.")

    def _validar(self, df: pd.DataFrame, nombre: str, cols: list):
        if df is None or df.empty:
            raise ValueError(f"[VALIDATE] DataFrame '{nombre}' vacío.")
        faltantes = [c for c in cols if c not in df.columns]
        if faltantes:
            raise ValueError(f"[VALIDATE] '{nombre}' le faltan columnas: {faltantes}")
        logger.info("[VALIDATE] '%s' OK — %d filas", nombre, len(df))
