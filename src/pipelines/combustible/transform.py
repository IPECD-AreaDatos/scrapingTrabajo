import pandas as pd
import numpy as np
import os
from src.shared.mappings import provinciass

def transform_combustible_data(file_path, provincia_filtrada=False):
    print("🔄 Transformando archivo venta_combustible.csv...")
    
    pd.set_option('mode.chained_assignment', None)

    # Leer el archivo CSV con control de errores
    try:
        df = pd.read_csv(file_path)
    except FileNotFoundError:
        raise FileNotFoundError(f"El archivo {file_path} no se encuentra en la ruta especificada.")
    except pd.errors.EmptyDataError:
        raise ValueError("El archivo está vacío.")
    except pd.errors.ParserError:
        raise ValueError("Hubo un error al parsear el archivo CSV.")
    
    # Aplicar transformaciones
    df = transformar_columnas(df)
    df = transformar_provincia(df)

    # Filtrar por provincia si se requiere
    if provincia_filtrada:
            df = df[df['provincia'] == 18]  # Solo Corrientes

    df = df.drop(columns=['unidad'])

    return df

def transformar_columnas(df):
    # Eliminar columnas innecesarias de forma más eficiente
    columnas_a_eliminar = ['empresa', 'tipodecomercializacion', 'subtipodecomercializacion', 'pais', 'indice_tiempo']
    df = df.drop(columns=columnas_a_eliminar)

    # Crear columna 'fecha' a partir de 'anio' y 'mes'
    df['fecha'] = pd.to_datetime(df['anio'].astype(str) + '-' + df['mes'].astype(str) + '-01')

    # Eliminar las columnas 'anio' y 'mes' ya que 'fecha' las reemplaza
    df = df.drop(columns=['anio', 'mes'])

    # Reordenar para poner 'fecha' como la primera columna
    df.insert(0, 'fecha', df.pop('fecha'))
        
    return df

def transformar_provincia(df):
    # Filtrar valores no deseados en 'provincia' de manera más eficiente
    provincias_no_deseadas = ['S/D', 'no aplica', 'Provincia']
    df = df[~df['provincia'].isin(provincias_no_deseadas)]

    # Reemplazar los nombres de provincias por sus códigos numéricos
    df['provincia'] = df['provincia'].replace(provinciass)

    # Filtrar solo las filas donde la provincia es 'Corrientes' (código 18)
    df_corrientes = df[df['provincia'] == 18]

    return df_corrientes
    
def suma_por_fecha(file_path):
    # Crear el DataFrame con los datos filtrados por el año
    df = transform_combustible_data(file_path)

    # Obtener la última fecha disponible
    ultima_fecha = df['fecha'].max()

    # Filtrar el DataFrame por la fecha proporcionada
    df_fecha = df[df['fecha'] == ultima_fecha]

    # Sumar las columnas numéricas
    suma = df_fecha['cantidad'].sum()  # Solo sumamos la columna 'cantidad'
    print(ultima_fecha)

    return suma  # Ahora retorna un valor numérico, no una Serie