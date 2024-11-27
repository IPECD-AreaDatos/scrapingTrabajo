import pandas as pd
import numpy as np
import os

# Obtener ruta del archivo CSV de manera más flexible
def obtener_ruta_archivo(nombre_archivo):
    directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
    return os.path.join(ruta_carpeta_files, nombre_archivo)

class Transformacion:
    def __init__(self, archivo='ventas_combustible.csv'):
        # Ruta al archivo CSV
        self.file_path = obtener_ruta_archivo(archivo)

    def crear_df(self):
        pd.set_option('mode.chained_assignment', None)
        # Leer el archivo CSV
        df = pd.read_csv(self.file_path)

        # Aplicar transformaciones
        df = self.transformar_columnas(df)
        df = self.transformar_provincia(df)

        # Mostrar columnas, tipos y valores únicos de la provincia para depuración
        #print(df.columns)
        #print(df.dtypes)
        #print(np.unique(df['provincia']))

        # Eliminar columna 'unidad' ya que no se necesita
        df = df.drop(columns=['unidad'])

        return df

    def transformar_columnas(self, df):
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

    def transformar_provincia(self, df):
        # Filtrar valores no deseados en 'provincia' de manera más eficiente
        provincias_no_deseadas = ['S/D', 'no aplica', 'Provincia']
        df = df[~df['provincia'].isin(provincias_no_deseadas)]

        # Diccionario para reemplazar provincias por sus códigos numéricos
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

        # Reemplazar los nombres de provincias por sus códigos numéricos
        df['provincia'] = df['provincia'].replace(dict_provincias)

        # Filtrar solo las filas donde la provincia es 'Corrientes' (código 18)
        df_corrientes = df[df['provincia'] == 18]

        return df_corrientes
