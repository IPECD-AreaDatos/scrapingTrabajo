import os
import pandas as pd

import logging
logger = logging.getLogger("emae") # Obtiene el logger ya configurado por run_emae
from src.shared.mappings import meses_esp


def transform_emae_valores():
    logger.info("Transformando archivo emae.xls...")

    # Ruta del archivo
    base_path = os.path.dirname(os.path.abspath(__file__))
    raw_folder = os.path.abspath(os.path.join(base_path, '../../data/raw/emae'))
    file_path = os.path.join(raw_folder, 'emae.xls')

    df = pd.read_excel(file_path, sheet_name=0, skiprows=4)

    # Eliminar última fila si contiene 'Fuente'
    if 'Fuente' in str(df.iloc[-1, 0]):
        df = df.drop(df.index[-1])

    # Rellenar los años hacia abajo
    df.iloc[:, 0] = df.iloc[:, 0].ffill()

    sectores = df.columns[2:].tolist()

    df['anio'] = df.iloc[:, 0].astype(str)
    df['mes'] = df.iloc[:, 1].map(meses_esp)
    df['fecha'] = pd.to_datetime(df['anio'] + '-' + df['mes'], format='%Y-%m', errors='coerce')
    df = df[df['fecha'].notna()]

    df = df.drop(columns=[df.columns[0], df.columns[1], 'anio', 'mes'])
    df = df[['fecha'] + [col for col in df.columns if col != 'fecha']]

    df_melted = df.melt(id_vars='fecha', var_name='sector', value_name='valor')
    sector_map = {name: i + 1 for i, name in enumerate(sectores)}
    df_melted['sector_productivo'] = df_melted['sector'].map(sector_map)
    df_melted = df_melted.dropna(subset=['valor'])

    df_melted['fecha'] = pd.to_datetime(df_melted['fecha']).dt.strftime('%Y-%m-%d')
    df_final = df_melted[['fecha', 'sector_productivo', 'valor']].sort_values(['fecha', 'sector_productivo'])

    logger.info("Transformacion de valores EMAE completada.")
    return df_final


def transform_emae_variaciones():
    logger.info("Transformando archivo emaevar.xls...")

    # Ruta del archivo
    base_path = os.path.dirname(os.path.abspath(__file__))
    raw_folder = os.path.abspath(os.path.join(base_path, '../../data/raw/emae'))
    file_path = os.path.join(raw_folder, 'emaevar.xls')

    df = pd.read_excel(file_path, sheet_name=0, skiprows=4, usecols="D,F")
    df.columns = ['variacion_interanual', 'variacion_mensual']
    df = df.fillna(0)
    df = df[(df != 0).any(axis=1)]

    fechas = pd.date_range(start='2004-02-01', periods=df.shape[0], freq='MS')
    df['fecha'] = fechas

    df_final = df[['fecha', 'variacion_interanual', 'variacion_mensual']]

    logger.info("Transformacion de variaciones EMAE completada.")
    return df_final
