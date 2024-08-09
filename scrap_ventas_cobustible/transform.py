import pandas as pd
import numpy as np
import os
import sys

#↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ LECTURA ARCHIVO CUALQUIER PC ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
file_path_desagregado = os.path.join(ruta_carpeta_files, 'ventas_combustible.csv')

class Transformacion: 
    # Objetivo: armar el df final con todos sus campos transformados correctamente
    def crear_df(self):
        df = pd.read_csv(file_path_desagregado)
        df = self.transformar_columnas(df)
        df = self.transformar_provincia(df)
        print(df.columns)
        print(df.dtypes)
        print(np.unique(df['provincia']))
        df = df.drop(columns=['unidad'])
        return df        

    # Objetivo: modficar cantidad de columna y editar columna fecha
    def transformar_columnas(self, df):
        df = df.drop(columns = ['empresa','tipodecomercializacion','subtipodecomercializacion','pais', 'indice_tiempo'])
        df['fecha'] = pd.to_datetime(df['anio'].astype(str) + '-' + df['mes'].astype(str) + '-01')
        df = df.drop(columns = ['anio','mes'])
        df.insert(0, 'fecha', df.pop('fecha'))
        return df

    # Objetivo: cambiar el nombre de las provincias por su codigo numerico representativo
    def transformar_provincia(self, df):
        df = df[df['provincia'] != 'S/D']
        df = df[df['provincia'] != 'no aplica']        
        df = df[df['provincia'] != 'Provincia']
        dict_provincias = {
            'Estado Nacional': 1,
            'Capital Federal': 2,
            'Buenos Aires': 6,
            'Catamarca': 10,
            'Chaco': 22,
            'Chubut': 26,
            'Córdoba': 14,
            'Corrientes': 18,
            'Entre Rios': 30,
            'Formosa': 34,
            'Jujuy': 38,
            'La Pampa': 42,
            'La Rioja': 46,
            'Mendoza': 50,
            'Misiones': 54,
            'Neuquén': 58,
            'Rio Negro': 62,
            'Salta': 66,
            'San Juan': 70,
            'San Luis': 74,
            'Santa Cruz': 78,
            'Santa Fe': 82,
            'Santiago del Estero': 86,
            'Tierra del Fuego': 94,
            'Tucuman': 90,
        }
        df['provincia'] = df['provincia'].replace(dict_provincias)
        return df
