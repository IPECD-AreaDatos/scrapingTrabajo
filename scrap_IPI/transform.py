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

        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        path_file = os.path.join(directorio_actual, 'files','IPI.xls')
        
        #DIRECCION DEL EXCEL
        self.path_excel = path_file

        #Dataframes a construir
        self.df_variaciones = pd.DataFrame() #Variaciones
        self.df_valores = pd.DataFrame() #Valores
        self.df_var_inter_acum = pd.DataFrame() #Variaciones Interanual Acumulada

    def construir_df_variaciones(self):

        #Nombre de columnas
        nombre_cols = ['var_IPI','var_interanual_alimentos','var_interanual_textil','var_interanual_maderas','var_interanual_sustancias','var_interanual_MinNoMetalicos','var_interanual_metales']

        # Crear el dataframe - especificamos, HOJA - QUE COLUMNAS USAMOS - LOS NOMBRES DE LAS COLUMNAS -LA FILA DONDE ARRANCA
        self.df_variaciones = pd.read_excel(self.path_excel,sheet_name='Cuadro 3',usecols='D,E,V,AE,AO,BB,BM',names=nombre_cols,skiprows=16)
        self.df_variaciones = self.df_variaciones.dropna()
        
        #Dividimos por 100 para obtener la variacion en terminos NO PORCENTUALES
        self.df_variaciones = self.df_variaciones / 100

        #Generador de fechas
        fecha_inicio = date(2017, 1,1)
        num_meses = len(self.df_variaciones)  # Restar 2 para compensar las filas de encabezados

        #Generamos una lista de fechas, teniendo en cuenta 'fecha_inicio'
        lista_fechas = [fecha_inicio + relativedelta(months=i) for i in range(num_meses)]

        self.df_variaciones['fecha'] = lista_fechas

        # Reordenar las columnas para que 'fecha' sea la primera
        self.df_variaciones = self.df_variaciones[['fecha'] + [col for col in self.df_variaciones.columns if col != 'fecha']]

    
    #Objetivo: tomar los valores de la serie original (hoja = "CUADRO 2")
    def construir_df_valores(self):

        #Nombre de columnas
        nombre_cols = ['ipi_manufacturero','alimentos','textil','maderas','sustancias','min_no_metalicos','metales']

        # Crear el dataframe - especificamos, HOJA - QUE COLUMNAS USAMOS - LOS NOMBRES DE LAS COLUMNAS -LA FILA DONDE ARRANCA
        self.df_valores = pd.read_excel(self.path_excel,sheet_name=2,usecols='D,E,V,AE,AO,BB,BM',names=nombre_cols,skiprows=5)
        self.df_valores = self.df_valores.dropna()

        #AGREGAMOS VARIACIONES MENSUALES
        self.df_valores['var_mensual_ipi_manufacturero'] = self.df_valores['ipi_manufacturero'].pct_change()
        self.df_valores['var_mensual_alimentos'] = self.df_valores['alimentos'].pct_change()
        self.df_valores['var_mensual_textil'] = self.df_valores['textil'].pct_change()
        self.df_valores['var_mensual_maderas'] = self.df_valores['maderas'].pct_change()
        self.df_valores['var_mensual_sustancias'] = self.df_valores['sustancias'].pct_change()
        self.df_valores['var_mensual_min_no_metalicos'] = self.df_valores['min_no_metalicos'].pct_change()
        self.df_valores['var_mensual_min_metales'] = self.df_valores['metales'].pct_change()
       

        #Agregamos las fechas
        fecha_inicio = date(2016, 1,1)
        num_meses = len(self.df_valores)  # Restar 2 para compensar las filas de encabezados

        #Generamos una lista de fechas, teniendo en cuenta 'fecha_inicio'
        lista_fechas = [fecha_inicio + relativedelta(months=i) for i in range(num_meses)]

        self.df_valores['fecha'] = lista_fechas

        # Reordenar las columnas para que 'fecha' sea la primera
        self.df_valores = self.df_valores[['fecha'] + [col for col in self.df_valores.columns if col != 'fecha']]


    def construir_df_acum_interanual(self):

        #Nombre de columnas
        nombre_cols = ['ipi_manufacturero_inter_acum','alimentos_inter_acum','textil_inter_acum',
                       'maderas_inter_acum','sustancias_inter_acum','min_no_metalicos_inter_acum','metales_inter_acum']

        # Crear el dataframe - especificamos, HOJA - QUE COLUMNAS USAMOS - LOS NOMBRES DE LAS COLUMNAS -LA FILA DONDE ARRANCA
        self.df_var_inter_acum = pd.read_excel(self.path_excel,sheet_name=4,usecols='D,E,V,AE,AO,BB,BM',names=nombre_cols,skiprows=16)
        self.df_var_inter_acum = self.df_var_inter_acum.dropna()

        #Agregamos las fechas
        fecha_inicio = date(2017, 1,1)
        num_meses = len(self.df_var_inter_acum)  # Restar 2 para compensar las filas de encabezados

        #Generamos una lista de fechas, teniendo en cuenta 'fecha_inicio'
        lista_fechas = [fecha_inicio + relativedelta(months=i) for i in range(num_meses)]

        self.df_var_inter_acum['fecha'] = lista_fechas

        # Reordenar las columnas para que 'fecha' sea la primera
        self.df_var_inter_acum = self.df_var_inter_acum[['fecha'] + [col for col in self.df_var_inter_acum.columns if col != 'fecha']]       

    
    def main(self):

        self.construir_df_valores()
        self.construir_df_variaciones()
        self.construir_df_acum_interanual()

        return self.df_valores,self.df_variaciones,self.df_var_inter_acum
    

