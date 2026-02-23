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
    """Transforma el XLS del IPI unificando valores y variaciones en un solo DF."""

    def transform(self, ruta_archivo: str = None) -> pd.DataFrame:
        if ruta_archivo is None:
            ruta_archivo = os.path.join(FILES_DIR, NOMBRE_ARCHIVO)

        logger.info(f"[TRANSFORM] Unificando datos del IPI desde: {ruta_archivo}")
        
        # 1. Obtener los 3 bloques de datos
        df_val = self._construir_df_valores(ruta_archivo)
        df_var = self._construir_df_variaciones(ruta_archivo)
        df_acu = self._construir_df_acum_interanual(ruta_archivo)

        # 2. Unificar (Merge) por fecha
        # Usamos inner merge para asegurar que solo suban meses que tengan todos los indicadores
        df_final = pd.merge(df_val, df_var, on='fecha', how='left')
        df_final = pd.merge(df_final, df_acu, on='fecha', how='left')

        # Redondear todo a 4 decimales para consistencia
        df_final = df_final.round(6)
        
        # Asegurar nombres en minúsculas para Postgres
        df_final.columns = [c.lower() for c in df_final.columns]

        logger.info(f"[OK] DataFrame unificado: {len(df_final)} filas y {len(df_final.columns)} columnas.")
        return df_final

    def _construir_df_valores(self, ruta: str) -> pd.DataFrame:
        cols = ['ipi_manufacturero', 'alimentos', 'textil', 'maderas', 'sustancias', 'min_no_metalicos', 'min_metales']
        df_ipi = pd.read_excel(ruta, sheet_name=1, usecols='H', skiprows=7, names=['ipi_manufacturero'])
        df_otros = pd.read_excel(ruta, sheet_name=2, usecols='E,V,AE,AO,BB,BM', names=cols[1:], skiprows=5)
        df = pd.concat([df_ipi, df_otros], axis=1).dropna()
        
        # Calcular variaciones mensuales
        for col in cols:
            df[f'var_mensual_{col}'] = df[col].pct_change()
            
        fecha_inicio = date(2016, 1, 1)
        df['fecha'] = [fecha_inicio + relativedelta(months=i) for i in range(len(df))]
        return df

    def _construir_df_variaciones(self, ruta: str) -> pd.DataFrame:
        cols = ['var_ipi', 'var_interanual_alimentos', 'var_interanual_textil', 'var_interanual_maderas', 
                'var_interanual_sustancias', 'var_interanual_min_no_metalicos', 'var_interanual_metales']
        df = pd.read_excel(ruta, sheet_name='Cuadro 3', usecols='D,E,V,AE,AO,BB,BM', names=cols, skiprows=16).dropna()
        df = df / 100
        fecha_inicio = date(2017, 1, 1)
        df['fecha'] = [fecha_inicio + relativedelta(months=i) for i in range(len(df))]
        return df

    def _construir_df_acum_interanual(self, ruta: str) -> pd.DataFrame:
        cols = ['ipi_manufacturero_inter_acum', 'alimentos_inter_acum', 'textil_inter_acum', 'maderas_inter_acum', 
                'sustancias_inter_acum', 'min_no_metalicos_inter_acum', 'metales_inter_acum']
        df = pd.read_excel(ruta, sheet_name=4, usecols='D,E,V,AE,AO,BB,BM', names=cols, skiprows=16).dropna()
        df = df / 100
        fecha_inicio = date(2017, 1, 1)
        df['fecha'] = [fecha_inicio + relativedelta(months=i) for i in range(len(df))]
        return df