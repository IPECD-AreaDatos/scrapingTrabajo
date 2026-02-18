"""
TRANSFORM - Módulo de transformación de datos IPC CABA
Responsabilidad: Leer y limpiar el Excel descargado
"""
import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')
NOMBRE_ARCHIVO = 'ipc_caba.xlsx'


class TransformIPCCABA:
    """Transforma el Excel del IPC CABA en un DataFrame limpio."""

    def transform(self, ruta_archivo: str = None) -> pd.DataFrame:
        """
        Lee el Excel y retorna un DataFrame con columnas [fecha, var_mensual_ipc_caba].

        Args:
            ruta_archivo: Ruta al xlsx. Si es None, usa la ruta por defecto en files/

        Returns:
            pd.DataFrame
        """
        if ruta_archivo is None:
            ruta_archivo = os.path.join(FILES_DIR, NOMBRE_ARCHIVO)

        logger.info("[TRANSFORM] Leyendo archivo: %s", ruta_archivo)
        df = pd.read_excel(ruta_archivo, skiprows=4, usecols='A,F')
        df = df.dropna().reset_index(drop=True)
        df.columns = ['fecha', 'var_mensual_ipc_caba']
        logger.info("[TRANSFORM] DataFrame generado: %d filas", len(df))
        return df
