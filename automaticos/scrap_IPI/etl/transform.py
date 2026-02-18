"""
TRANSFORM - Módulo de transformación de datos IPI
Responsabilidad: Construir los 3 DataFrames del IPI (valores, variaciones, acumulado)
"""
import os
import logging
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')
NOMBRE_ARCHIVO = 'IPI.xls'


class TransformIPI:
    """Transforma el XLS del IPI en 3 DataFrames."""

    def transform(self, ruta_archivo: str = None) -> tuple:
        """
        Construye los 3 DataFrames del IPI.

        Returns:
            tuple: (df_valores, df_variaciones, df_var_inter_acum)
        """
        if ruta_archivo is None:
            ruta_archivo = os.path.join(FILES_DIR, NOMBRE_ARCHIVO)

        logger.info("[TRANSFORM] Procesando IPI desde: %s", ruta_archivo)
        df_valores = self._construir_df_valores(ruta_archivo)
        df_variaciones = self._construir_df_variaciones(ruta_archivo)
        df_var_inter_acum = self._construir_df_acum_interanual(ruta_archivo)
        logger.info("[TRANSFORM] Valores: %d filas | Variaciones: %d | Acum: %d",
                    len(df_valores), len(df_variaciones), len(df_var_inter_acum))
        return df_valores, df_variaciones, df_var_inter_acum

    def _construir_df_valores(self, ruta: str) -> pd.DataFrame:
        nombre_cols = ['ipi_manufacturero', 'alimentos', 'textil', 'maderas',
                       'sustancias', 'min_no_metalicos', 'min_metales']
        df_ipi = pd.read_excel(ruta, sheet_name=1, usecols='H', skiprows=7,
                               names=['ipi_manufacturero'])
        df_otros = pd.read_excel(ruta, sheet_name=2, usecols='E,V,AE,AO,BB,BM',
                                 names=nombre_cols[1:], skiprows=5)
        df = pd.concat([df_ipi, df_otros], axis=1).dropna()
        for col in nombre_cols:
            df[f'var_mensual_{col}'] = df[col].pct_change()
        fecha_inicio = date(2016, 1, 1)
        df['fecha'] = [fecha_inicio + relativedelta(months=i) for i in range(len(df))]
        return df[['fecha'] + [c for c in df.columns if c != 'fecha']]

    def _construir_df_variaciones(self, ruta: str) -> pd.DataFrame:
        nombre_cols = ['var_IPI', 'var_interanual_alimentos', 'var_interanual_textil',
                       'var_interanual_maderas', 'var_interanual_sustancias',
                       'var_interanual_MinNoMetalicos', 'var_interanual_metales']
        df = pd.read_excel(ruta, sheet_name='Cuadro 3', usecols='D,E,V,AE,AO,BB,BM',
                           names=nombre_cols, skiprows=16).dropna()
        df /= 100
        fecha_inicio = date(2017, 1, 1)
        df['fecha'] = [fecha_inicio + relativedelta(months=i) for i in range(len(df))]
        return df[['fecha'] + [c for c in df.columns if c != 'fecha']]

    def _construir_df_acum_interanual(self, ruta: str) -> pd.DataFrame:
        nombre_cols = ['ipi_manufacturero_inter_acum', 'alimentos_inter_acum',
                       'textil_inter_acum', 'maderas_inter_acum', 'sustancias_inter_acum',
                       'min_no_metalicos_inter_acum', 'metales_inter_acum']
        df = pd.read_excel(ruta, sheet_name=4, usecols='D,E,V,AE,AO,BB,BM',
                           names=nombre_cols, skiprows=16).dropna()
        fecha_inicio = date(2017, 1, 1)
        df['fecha'] = [fecha_inicio + relativedelta(months=i) for i in range(len(df))]
        return df[['fecha'] + [c for c in df.columns if c != 'fecha']]
