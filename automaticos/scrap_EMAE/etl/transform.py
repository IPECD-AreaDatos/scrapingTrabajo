"""
TRANSFORM - Módulo de transformación de datos EMAE
Responsabilidad: Construir los 2 DataFrames del EMAE (valores y variaciones)
"""
import os
import logging
import pandas as pd

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')

MESES = {
    'Enero': '01', 'Febrero': '02', 'Marzo': '03', 'Abril': '04',
    'Mayo': '05', 'Junio': '06', 'Julio': '07', 'Agosto': '08',
    'Septiembre': '09', 'Octubre': '10', 'Noviembre': '11', 'Diciembre': '12'
}


class TransformEMAE:
    """Transforma los 2 XLS del EMAE en DataFrames listos para cargar."""

    def transform(self, ruta_valores: str = None, ruta_variaciones: str = None) -> tuple:
        """
        Construye los 2 DataFrames del EMAE.

        Returns:
            tuple: (df_valores, df_variaciones)
        """
        if ruta_valores is None:
            ruta_valores = os.path.join(FILES_DIR, 'emae.xls')
        if ruta_variaciones is None:
            ruta_variaciones = os.path.join(FILES_DIR, 'emaevar.xls')

        logger.info("[TRANSFORM] Procesando EMAE valores...")
        df_val = self._construir_df_valores(ruta_valores)
        logger.info("[TRANSFORM] Procesando EMAE variaciones...")
        df_var = self._construir_df_variaciones(ruta_variaciones)
        logger.info("[TRANSFORM] Valores: %d filas | Variaciones: %d filas", len(df_val), len(df_var))
        return df_val, df_var

    def _construir_df_valores(self, ruta: str) -> pd.DataFrame:
        pd.set_option('future.no_silent_downcasting', True)
        df = pd.read_excel(ruta, sheet_name=0, skiprows=2)

        # Eliminar filas con "Fuente"
        mask_fuente = df.astype(str).apply(lambda x: x.str.contains('Fuente', na=False)).any(axis=1)
        df = df[~mask_fuente]

        year_col  = df.columns[0]
        month_col = df.columns[1]
        sectores  = df.columns[2:].tolist()

        df[year_col] = df[year_col].ffill()
        df['anio_str'] = df[year_col].fillna(0).astype(int).astype(str).replace('0', '')
        df['mes_num']  = df[month_col].map(MESES)

        mask_valid = (
            df[year_col].notna() &
            df[month_col].notna() &
            df['mes_num'].notna() &
            (df['anio_str'] != '')
        )
        df.loc[mask_valid, 'fecha'] = pd.to_datetime(
            df.loc[mask_valid, 'anio_str'] + '-' + df.loc[mask_valid, 'mes_num'],
            format='%Y-%m', errors='coerce'
        )
        df = df[df['fecha'].notna()].copy()
        df = df[['fecha'] + sectores]

        df_melted = df.melt(id_vars='fecha', var_name='sector', value_name='valor')
        sector_map = {name: i+1 for i, name in enumerate(sectores)}
        df_melted['sector_productivo'] = df_melted['sector'].map(sector_map)
        df_melted = df_melted.dropna(subset=['valor'])
        df_melted['fecha'] = pd.to_datetime(df_melted['fecha']).dt.strftime('%Y-%m-%d')
        return df_melted[['fecha', 'sector_productivo', 'valor']].sort_values(
            ['fecha', 'sector_productivo']
        ).reset_index(drop=True)

    def _construir_df_variaciones(self, ruta: str) -> pd.DataFrame:
        df = pd.read_excel(ruta, sheet_name=0, skiprows=4, usecols="D,F")
        df.columns = ['variacion_interanual', 'variacion_mensual']
        df = df.fillna(0)
        df = df[(df != 0).any(axis=1)]
        df['fecha'] = pd.date_range(start='2004-02-01', periods=len(df), freq='MS')
        return df[['fecha', 'variacion_interanual', 'variacion_mensual']]
