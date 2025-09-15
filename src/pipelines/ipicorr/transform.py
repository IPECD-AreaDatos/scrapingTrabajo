import os
import pandas as pd
from src.shared.logger import setup_logger
from src.shared.mappings import meses_abbr

logger = setup_logger("ipicorr")
def transform_ipicorr(df_raw: pd.DataFrame) -> pd.DataFrame:

    columnas = ['Fecha', 'Var_ia_Nivel_General', 'Vim_Nivel_General', 'Vim_Alimentos', 'Vim_Textil',
                'Vim_Maderas', 'Vim_min_nometalicos', 'Vim_metales', 'Var_ia_Alimentos', 'Var_ia_Textil',
                'Var_ia_Maderas', 'Var_ia_min_nometalicos', 'Var_ia_metales', 'Estatus']

    df_raw = df_raw.iloc[:, :14]
    df_raw.columns = columnas
    df_raw = df_raw[df_raw['Estatus'].notnull()]  # Solo filas completas (con 'Finalizado')

    df_raw['Fecha'] = df_raw['Fecha'].apply(convertir_fecha)
    df = df_raw.drop(columns=['Estatus']).iloc[11:].reset_index(drop=True)

    columnas_porcentaje = df.columns.drop('Fecha')
    for col in columnas_porcentaje:
        df[col] = df[col].apply(convertir_a_float)

    return df

def convertir_a_float(valor):
    try:
        valor = valor.replace('%', '').replace(',', '.').strip()
        return float(valor) / 100
    except:
        return None
    
def convertir_fecha(fecha_str):
        try:
            partes = fecha_str.strip().lower().split('-')
            mes_abbr = partes[0][:3]  # 'septiembre' → 'sep', etc.
            anio = partes[1]
            mes_numero = meses_abbr.get(mes_abbr, '00')
            return f"{anio}-{mes_numero}-01"
        except Exception as e:
            logger.warning(f"❗ Error al convertir fecha: {fecha_str} → {e}")
            return '0000-00-01'