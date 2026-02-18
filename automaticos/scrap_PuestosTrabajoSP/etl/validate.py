"""
VALIDATE - Módulo de validación de datos PuestosTrabajoSP
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidatePuestosTrabajoSP:
    def validate(self, df_privado: pd.DataFrame, df_total: pd.DataFrame):
        self._validar(df_privado, "privado")
        self._validar(df_total,   "total")
        logger.info("[VALIDATE] OK — PuestosTrabajoSP validado.")

    def _validar(self, df: pd.DataFrame, nombre: str):
        if df is None or df.empty:
            raise ValueError(f"[VALIDATE] DataFrame '{nombre}' vacío.")
        logger.info("[VALIDATE] '%s' OK — %d filas", nombre, len(df))
