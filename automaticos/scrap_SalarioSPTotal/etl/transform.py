"""
TRANSFORM - Módulo de transformación de datos SalarioSPTotal
Responsabilidad: Leer y limpiar los 2 CSVs de salarios
"""
import os
import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')


class TransformSalarioSPTotal:
    """Transforma los 2 CSVs de salarios."""

    def transform(self, ruta_sp: str = None, ruta_total: str = None) -> tuple:
        """
        Lee y limpia ambos CSVs.

        Returns:
            tuple: (df_sp, df_total)
        """
        if ruta_sp is None:
            ruta_sp = os.path.join(FILES_DIR, 'salarioPromedioSP.csv')
        if ruta_total is None:
            ruta_total = os.path.join(FILES_DIR, 'salarioPromedioTotal.csv')

        logger.info("[TRANSFORM] Procesando CSV sector privado: %s", ruta_sp)
        df_sp = self._limpiar(ruta_sp)
        logger.info("[TRANSFORM] Procesando CSV total: %s", ruta_total)
        df_total = self._limpiar(ruta_total)
        return df_sp, df_total

    @staticmethod
    def _limpiar(ruta: str) -> pd.DataFrame:
        df = pd.read_csv(ruta)
        df = df.replace({np.nan: None})
        df.loc[df['w_mean'] < 0, 'w_mean'] = 0
        df = df.rename(columns={'w_mean': 'salario'})
        return df
