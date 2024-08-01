"""
Objetivo del archivo: construir el DF con el formato correspondiente indicado.
"""
import os
from dateutil.relativedelta import relativedelta
from datetime import date
import pandas as pd

class Transform:

    #Definicion de atribitos
    def __init__(self):
        pass

    def construir_df(self):

        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        path_file = os.path.join(directorio_actual, 'files','IPI.xls')

        #Nombre de columnas
        nombre_cols = ['var_IPI','var_interanual_alimentos','var_interanual_textil','var_interanual_maderas','var_interanual_sustancias','var_interanual_MinNoMetalicos','var_interanual_metales']

        # Crear el dataframe - especificamos, HOJA - QUE COLUMNAS USAMOS - LOS NOMBRES DE LAS COLUMNAS -LA FILA DONDE ARRANCA
        self.df = pd.read_excel(path_file,sheet_name='Cuadro 3',usecols='D,E,V,AE,AO,BB,BM',names=nombre_cols,skiprows=16)
        self.df = self.df.dropna()
        
        #Dividimos por 100 para obtener la variacion en terminos NO PORCENTUALES
        self.df = self.df / 100

        #Generador de fechas
        fecha_inicio = date(2017, 1,1)
        num_meses = len(self.df)  # Restar 2 para compensar las filas de encabezados

        #Generamos una lista de fechas, teniendo en cuenta 'fecha_inicio'
        lista_fechas = [fecha_inicio + relativedelta(months=i) for i in range(num_meses)]

        self.df['fecha'] = lista_fechas

        # Reordenar las columnas para que 'fecha' sea la primera
        self.df = self.df[['fecha'] + [col for col in self.df.columns if col != 'fecha']]

        return self.df



