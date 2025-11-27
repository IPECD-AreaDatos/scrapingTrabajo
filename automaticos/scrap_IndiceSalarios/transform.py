import os
import pandas as pd
from datetime import datetime

class Transformer:

    def __init__(self):
        
        df = None

    #Objetivo: Extraer los datos del CSV descargado
    def extract_data(self):

        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        #Construccion de direccion para acceder al csv
        path_csv = os.path.join(carpeta_guardado,"indice_salarios.csv")

        self.df = pd.read_csv(path_csv,delimiter=';')

    #Objetivo: remplazar las "," por ".". Ademas transformamos el tipo de dato a FLOAT
    def especial_characters(self):

        df_transformed = self.df.copy()

        for column in df_transformed:

            try:
                #Remplazamos "," por "." y pasamos de "str" a "float"
                df_transformed[column] = df_transformed[column].str.replace(",",".").astype(float)

            except:
                pass


        self.df = df_transformed
        
    #Objetivo: transformas las fechas a un formato "AÑO-MES-DIA"
    def transform_dates(self):
        
        # Parsear la fecha utilizando el formato día/mes/año
        self.df['periodo'] = pd.to_datetime(self.df['periodo'], format='%d/%m/%Y')

    #Main de la clase Transformer
    def transform_data_main(self):

        self.extract_data()
        self.especial_characters()
        self.transform_dates()

        # Convertir los nombres de las columnas a minúsculas
        self.df = self.df.rename(columns=lambda x: x.lower())

        return self.df

