"""
TRANSFORM - Módulo de transformación de datos PuestosTrabajoSP
Responsabilidad: Leer y limpiar los 2 CSVs de puestos de trabajo
"""
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class TransformPuestosTrabajoSP:
    """Transforma los 2 CSVs de puestos de trabajo."""

    def transform(self, ruta_privado: str, ruta_total: str) -> tuple:
        """
        Lee y limpia los 2 CSVs.

        Returns:
            tuple: (df_privado, df_total)
        """
        df_privado = self._limpiar(pd.read_csv(ruta_privado), 'privado')
        df_total   = self._limpiar(pd.read_csv(ruta_total),   'total')
        logger.info("[TRANSFORM] Privado: %d filas | Total: %d filas", len(df_privado), len(df_total))
        return df_privado, df_total

    def _limpiar(self, df: pd.DataFrame, nombre: str) -> pd.DataFrame:
        df = df.replace({np.nan: None})
        df = df.replace(-99, 0)
        logger.info("[TRANSFORM] '%s' limpiado: %d filas", nombre, len(df))
        return df
