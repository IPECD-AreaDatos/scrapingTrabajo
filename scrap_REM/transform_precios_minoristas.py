import pandas as pd
import numpy as np
import os
import sys

#↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ LECTURA ARCHIVO CUALQUIER PC ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
file_path_desagregado = os.path.join(ruta_carpeta_files, 'relevamiento_expectativas_mercado.xlsx')

class Transform1: 

    def crear_df_precios_minoristas(self):
        df = pd.read_excel(file_path_desagregado, skiprows=5)
        df = self.columnas(df)
        print(df.columns)
        print(df.dtypes)

        return df      
  


    def columnas(self, df):
        columnas_eliminar = ['Unnamed: 0', 'Referencia', 'Promedio', 'Desvío', 'Máximo','Mínimo', 'Percentil 90', 'Percentil 75', 'Percentil 25','Percentil 10', 'Cantidad de participantes']
        df = df.drop(columnas_eliminar, axis=1)
        df = df.iloc[:-94]
        df = df.rename(columns={'Período':'fecha_resguardo', 'Mediana':'mediana'})
        df.insert(1, 'fecha', df['fecha_resguardo'])
        df.insert(0, 'id', range(1, len(df) + 1))
        df['fecha_resguardo'] = df['fecha_resguardo'].astype(str)
        df['mediana'] = df['mediana'].astype(float)
        df.loc[len(df)-5:, 'fecha'] = ['2025-04-30 00:00:00', '2026-04-30 00:00:00', '2024-01-30 00:00:00', '2025-01-30 00:00:00', '2026-01-30 00:00:00']
        df['fecha'] = pd.to_datetime(df['fecha'])


        return df
      
