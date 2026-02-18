"""
TRANSFORM - Módulo de transformación de datos SalarioMVM
Responsabilidad: Leer el CSV y convertir fechas al formato correcto
"""
import os
import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')


class TransformSalarioMVM:
    """Transforma el CSV del Salario MVM."""

    def transform(self, ruta: str = None) -> pd.DataFrame:
        """
        Lee y limpia el CSV del salario mínimo.

        Returns:
            pd.DataFrame con columnas: indice_tiempo, salario_mvm_mensual, etc.
        """
        if ruta is None:
            ruta = os.path.join(FILES_DIR, 'salario_minimo.csv')

        logger.info("[TRANSFORM] Leyendo CSV: %s", ruta)
        df = pd.read_csv(ruta)
        df['indice_tiempo'] = df['indice_tiempo'].apply(
            lambda f: datetime.strptime(f, '%Y-%m-%d').date()
        )
        logger.info("[TRANSFORM] SalarioMVM: %d filas", len(df))
        return df
