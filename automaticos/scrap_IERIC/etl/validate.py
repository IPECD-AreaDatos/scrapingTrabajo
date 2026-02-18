"""
VALIDATE - Módulo de validación de datos IERIC
"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class ValidateIERIC:
    def validate(self, df_actividad: pd.DataFrame, df_puestos: pd.DataFrame,
                 df_salario: pd.DataFrame):
        self._validar(df_actividad, "actividad", ['fecha', 'id_provincia', 'cant_empresas'])
        self._validar(df_puestos,   "puestos",   ['fecha', 'id_provincia', 'puestos_de_trabajo'])
        self._validar(df_salario,   "salario",   ['fecha', 'id_provincia', 'salario_promedio'])
        logger.info("[VALIDATE] OK — todos los DataFrames del IERIC son válidos.")

    def _validar(self, df: pd.DataFrame, nombre: str, cols: list):
        if df is None or df.empty:
            raise ValueError(f"[VALIDATE] DataFrame '{nombre}' vacío.")
        faltantes = [c for c in cols if c not in df.columns]
        if faltantes:
            raise ValueError(f"[VALIDATE] '{nombre}' le faltan columnas: {faltantes}")
        logger.info("[VALIDATE] '%s' OK — %d filas", nombre, len(df))
