"""
Transformer para procesar datos de CBA y CBT.

En este script manejamos dos hojas del mismo excel CBT.xls y extraemos datos del NEA desde Pobreza.xls:
"primera_hoja": corresponde a los datos de CBA y CBT de ADULTOS. Es decir, por individuo.
"segunda_hoja": corresponde a los datos de CBA y CBT de FAMILIAS. El tipo de familia es el 2, que es un grupo de 4 personas
"datos_nea": se extraen desde Pobreza.xls, hoja "Series canastas anexo", región Noreste
"""
import os
import logging
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

class TransformerCBTCBA:
    """
    Procesa los archivos descargados y genera un DataFrame consolidado.
    """

    def __init__(self):
        # Centralizamos la configuración de rutas
        self.base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.data_dir = os.path.join(self.base_dir, 'files', 'data')
        self.file_cbt = os.path.join(self.data_dir, 'CBT.xls')
        self.file_pobreza = os.path.join(self.data_dir, 'Pobreza.xls')

    def transform_datalake(self):
        """Coordina la transformación completa."""
        logger.info("[TRANSFORM] Iniciando procesamiento...")

        # 1. Procesar Hoja 1 (Adultos)
        # Cargamos directamente las columnas necesarias y saltamos basura
        df_adultos = pd.read_excel(self.file_cbt, sheet_name=0, usecols=[0, 1, 3], skiprows=6, skipfooter=1)
        df_adultos.columns = ['fecha', 'cba_adulto', 'cbt_adulto']
        
        # Limpieza de valores (Reemplazo masivo de comas y corrección de outliers)
        for col in ['cba_adulto', 'cbt_adulto']:
            df_adultos[col] = pd.to_numeric(df_adultos[col].astype(str).str.replace(',', ''), errors='coerce')

        # Corrección específica de valores (como tenías en tu script)
        # Si el valor es masivo (ej: 13 millones), lo dividimos por 100
        mask_cba = df_adultos['cba_adulto'] == 13874431
        df_adultos.loc[mask_cba, 'cba_adulto'] = 138744.31
        mask_cbt = df_adultos['cbt_adulto'] == 31217470
        df_adultos.loc[mask_cbt, 'cbt_adulto'] = 312174.70

        # 2. Procesar Hoja 2 (Hogares)
        df_hogares = pd.read_excel(self.file_cbt, sheet_name=3, usecols=[2, 6], skiprows=6, skipfooter=1)
        df_hogares.columns = ['cba_hogar', 'cbt_hogar']

        # 3. Limpieza de filas vacías
        df_adultos = df_adultos.dropna(subset=['fecha']).reset_index(drop=True)
        df_hogares = df_hogares.dropna(how='all').reset_index(drop=True)

        # 4. Concatenar bases de CBT.xls (Aseguramos mismo largo)
        df_base = pd.concat([df_adultos, df_hogares], axis=1)
        df_base['fecha'] = pd.to_datetime(df_base['fecha'])

        # 5. Extraer y Estimar datos del NEA
        df_nea = self.extraer_datos_nea(self.file_pobreza, len(df_base))
        df_full = pd.concat([df_base, df_nea], axis=1)

        return self.calcular_estimaciones_nea(df_full)

    def extraer_datos_nea(self, path, total_rows):
        """Extrae datos del NEA desde Pobreza.xls."""
        try:
            df_raw = pd.read_excel(path, sheet_name='Series canastas anexo', skiprows=5)
            
            # Filtro por texto 'Noreste'
            df_nea_rows = df_raw[df_raw.iloc[:, 0].astype(str).str.contains('Noreste', na=False, case=False)]
            
            if len(df_nea_rows) < 2:
                return self._df_vacio_nea(total_rows)

            # Extraemos valores (CBA es la primera fila del NEA, Engel la segunda)
            valores_cba = pd.to_numeric(df_nea_rows.iloc[0, 1:], errors='coerce').dropna().values
            coef_engel = pd.to_numeric(df_nea_rows.iloc[1, 1:], errors='coerce').dropna().values
            
            # CBT NEA = CBA * Coeficiente de Engel
            valores_cbt = valores_cba * coef_engel
            
            # Creamos el DF alineado al final (datos más recientes)
            df_res = self._df_vacio_nea(total_rows)
            start_idx = total_rows - len(valores_cba)
            
            if start_idx >= 0:
                df_res.loc[start_idx:, 'cba_nea'] = valores_cba
                df_res.loc[start_idx:, 'cbt_nea'] = valores_cbt
            
            return df_res

        except Exception as e:
            logger.error(f"[TRANSFORM] Error NEA: {e}")
            return self._df_vacio_nea(total_rows)

    def calcular_estimaciones_nea(self, df):
        """Calcula estimaciones para meses sin datos oficiales del NEA."""
        # Fecha de corte (Junio 2025 según tu lógica)
        corte = pd.to_datetime("2025-06-01")
        
        # Si no hay fechas posteriores al corte, devolvemos como está
        if not (df['fecha'] > corte).any():
            return df

        # Calculamos ratios usando los últimos 6 meses oficiales
        ultimos_oficiales = df[df['fecha'] <= corte].tail(6)
        
        sumas = {
            'cba_gba': ultimos_oficiales['cba_adulto'].sum(),
            'cbt_gba': ultimos_oficiales['cbt_adulto'].sum(),
            'cba_nea': ultimos_oficiales['cba_nea'].sum(),
            'cbt_nea': ultimos_oficiales['cbt_nea'].sum()
        }

        if any(v == 0 for v in sumas.values()):
            logger.warning("No hay datos suficientes para estimar NEA.")
            return df

        ratio_cba = sumas['cba_nea'] / sumas['cba_gba']
        ratio_cbt = sumas['cbt_nea'] / sumas['cbt_gba']

        # Aplicamos la estimación vectorizada (mucho más rápido que un loop)
        mask_estimar = df['fecha'] > corte
        df.loc[mask_estimar, 'cba_nea'] = df.loc[mask_estimar, 'cba_adulto'] * ratio_cba
        df.loc[mask_estimar, 'cbt_nea'] = df.loc[mask_estimar, 'cbt_adulto'] * ratio_cbt

        return df

    def _df_vacio_nea(self, n):
        return pd.DataFrame({'cba_nea': [np.nan]*n, 'cbt_nea': [np.nan]*n})