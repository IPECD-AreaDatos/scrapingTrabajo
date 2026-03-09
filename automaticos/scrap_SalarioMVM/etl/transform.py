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
        # 1. Renombra las columnas al nombre exacto de la base de datos
        df = df.rename(columns={
            'indice_tiempo': 'fecha',
            'salario_minimo_vital_movil_mensual': 'salario_mvm_mensual', # Asegura que coincidan
            'salario_minimo_vital_movil_diario': 'salario_mvm_diario',
            'salario_minimo_vital_movil_hora': 'salario_mvm_hora'
        })
        
        # 2. Asegura el formato de fecha
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date
        logger.info("[TRANSFORM] SalarioMVM: %d filas", len(df))
        return df
