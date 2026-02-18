"""
TRANSFORM - Módulo de transformación de datos REM
Responsabilidad: Procesar los archivos Excel del REM (precios minoristas y cambio nominal)
"""
import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')
ARCHIVO_REM     = 'relevamiento_expectativas_mercado.xlsx'
ARCHIVO_HIST    = 'historico_rem.xlsx'

COLUMNAS_ELIMINAR = [
    'Unnamed: 0', 'Referencia', 'Promedio', 'Desvío', 'Máximo', 'Mínimo',
    'Percentil 90', 'Percentil 75', 'Percentil 25', 'Percentil 10',
    'Cantidad de participantes'
]


class TransformREM:
    """Transforma los archivos Excel del REM en DataFrames listos para cargar."""

    def transform(self) -> tuple:
        """
        Procesa ambos DataFrames del REM.

        Returns:
            tuple: (df_precios_minoristas, df_cambio_nominal)
        """
        logger.info("[TRANSFORM] Procesando precios minoristas...")
        df_precios = self._crear_df_precios_minoristas()
        logger.info("[TRANSFORM] Procesando cambio nominal...")
        df_cambio  = self._get_historico_cambio_nominal()
        return df_precios, df_cambio

    def _crear_df_precios_minoristas(self) -> pd.DataFrame:
        ruta = os.path.join(FILES_DIR, ARCHIVO_REM)
        df = pd.read_excel(ruta, skiprows=5)
        df = df.drop(COLUMNAS_ELIMINAR, axis=1)
        df = df.iloc[:-94]
        df = df.rename(columns={'Período': 'fecha_resguardo', 'Mediana': 'mediana'})
        df.insert(1, 'fecha', df['fecha_resguardo'])
        df.insert(0, 'id', range(1, len(df) + 1))
        df['fecha_resguardo'] = df['fecha_resguardo'].astype(str)
        df['mediana'] = df['mediana'].astype(float)
        df.loc[len(df)-5:, 'fecha'] = [
            '2025-04-30 00:00:00', '2026-04-30 00:00:00',
            '2024-01-30 00:00:00', '2025-01-30 00:00:00', '2026-01-30 00:00:00'
        ]
        df['fecha'] = pd.to_datetime(df['fecha'])
        logger.info("[TRANSFORM] Precios minoristas: %d filas", len(df))
        return df

    def _get_historico_cambio_nominal(self) -> pd.DataFrame:
        ruta = os.path.join(FILES_DIR, ARCHIVO_HIST)
        df = pd.read_excel(ruta, sheet_name=1, skiprows=1)
        df_filtrado = df[
            (df['Variable'] == 'Tipo de cambio nominal') & (df['Referencia'] == '$/USD')
        ]
        df_sel = df_filtrado.iloc[:, [0, 4]].copy()
        df_sel.columns = ['fecha', 'cambio_nominal']
        df_sel = df_sel.drop_duplicates(subset=['fecha'])

        fecha_max = df_sel['fecha'].max()
        last_data = df_sel[df_sel['fecha'] == fecha_max]
        next_month = fecha_max.month + 1 if fecha_max.month < 12 else 1
        next_year  = fecha_max.year if fecha_max.month < 12 else fecha_max.year + 1

        if len(last_data) > 1:
            prox = last_data.iloc[[1]].copy()
            prox['fecha'] = pd.to_datetime(f'{next_year}-{next_month}-01')
            df_sel = pd.concat([df_sel, prox])

        df_sel = df_sel.sort_values('fecha').tail(12).reset_index(drop=True)
        logger.info("[TRANSFORM] Cambio nominal: %d filas", len(df_sel))
        return df_sel
