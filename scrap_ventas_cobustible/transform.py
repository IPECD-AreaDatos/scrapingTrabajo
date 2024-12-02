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

    def crear_df(self, provincia_filtrada=False, ano_filtrado=2024, mes_filtrado=10):
        pd.set_option('mode.chained_assignment', None)

        # Leer el archivo CSV con control de errores
        try:
            df = pd.read_csv(self.file_path)
        except FileNotFoundError:
            raise FileNotFoundError(f"El archivo {self.file_path} no se encuentra en la ruta especificada.")
        except pd.errors.EmptyDataError:
            raise ValueError("El archivo está vacío.")
        except pd.errors.ParserError:
            raise ValueError("Hubo un error al parsear el archivo CSV.")
        
        # Filtrar por año (solo 2024 en este caso)
        df = df[df['anio'] == ano_filtrado]
        df = df[df['mes'] == mes_filtrado]

        # Aplicar transformaciones
        df = self.transformar_columnas(df)
        df = self.transformar_provincia(df)

        # Filtrar por provincia si se requiere
        if provincia_filtrada:
            df = df[df['provincia'] == 18]  # Solo Corrientes

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
    def suma_por_fecha(self, fecha='2024-09-01'):
        # Crear el DataFrame con los datos filtrados por el año
        df = self.crear_df()

        # Filtrar el DataFrame por la fecha proporcionada
        df_fecha = df[df['fecha'] == fecha]

        # Sumar las columnas numéricas
        suma = df_fecha['cantidad'].sum()  # Solo sumamos la columna 'cantidad'

        return suma  # Ahora retorna un valor numérico, no una Serie
