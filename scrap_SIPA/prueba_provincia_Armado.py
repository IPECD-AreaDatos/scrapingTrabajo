import datetime
import mysql.connector
import time
import numpy as np
import pandas as pd
import os
from homePage import HomePage


#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

url = HomePage()
directorio_actual = os.path.dirname(os.path.abspath(__file__))
ruta_carpeta_files = os.path.join(directorio_actual, 'files')
file_path = os.path.join(ruta_carpeta_files, 'SIPA.xlsx')


df_aux = pd.DataFrame()
lista_provincias = list() #--> Contendra los indices de la provincia
lista_valores_estacionalidad = list() #--> Contendra los valores 
lista_valores_sin_estacionalidad = list()
lista_registro = list()#--> Contendra el tipo de REGISTRO
lista_fechas= list() #--> Contendra las fechas

class LoadXLS5_1:
    def loadInDataBase(self, file_path, host, user, password, database):
        # Se toma el tiempo de comienzo
        start_time = time.time()

        try:

            # Leer el archivo Excel en un DataFrame de pandas
            df = pd.read_excel(file_path, sheet_name=13, skiprows=1)  # Leer el archivo XLSX y crear el DataFrame
            df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

            # Reemplazar comas por puntos en los valores numéricos
            df = df.replace(',', '.', regex=True)   
            df = df.iloc[:-6]#Elimina las ultimas 6 filas siempre
            df.drop(df.columns[-1], axis=1, inplace=True)#Elimina la ultima columna
            df = df.rename(columns=lambda x: x.strip())#Eliminar los espacion al final del nombre
            start_date = pd.to_datetime('2009-01-01')
            df['Período'] = pd.date_range(start=start_date, periods=len(df), freq='M').date



            
            #Leer el archivo Excel en un DataFrame de pandas
            df_NoEstacional = pd.read_excel(file_path, sheet_name=14, skiprows=1)  # Leer el archivo XLSX y crear el DataFrame
            df_NoEstacional = df_NoEstacional.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

            # Reemplazar comas por puntos en los valores numéricos
            df_NoEstacional = df_NoEstacional.replace(',', '.', regex=True)   
            df_NoEstacional = df_NoEstacional.iloc[:-7]#Elimina las ultimas 6 filas siempre
            df_NoEstacional.drop(df_NoEstacional.columns[-1], axis=1, inplace=True)#Elimina la ultima columna
            df_NoEstacional = df_NoEstacional.rename(columns=lambda x: x.strip())#Eliminar los espacion al final del nombre
            start_date = pd.to_datetime('2009-01-01')
            df_NoEstacional['Período'] = pd.date_range(start=start_date, periods=len(df_NoEstacional), freq='M').date
            


            global lista_valores_estacionalidad,lista_provincias,df_aux
             
            """
            #Datos de Buenos AIRES
            for i in df['BUENOS AIRES']:
                
                
                lista_provincias.append(6) #--> Carga de provincia POR ID - BUENOS AIRES: 6
                lista_valores_estacionalidad.append(i) #--> Carga de valor POR ESTACIONALDIAD
                lista_registro.append(1)#--> Carga de tipo de registro - TIPO EMPLEO: 1

            for i in df_NoEstacional['BUENOS AIRES']: #--> Carga de datos no ESTACIONALES

                lista_valores_sin_estacionalidad.append(i)

            """
            for (_, row), (_, row_no_estacional) in zip(df.iterrows(), df_NoEstacional.iterrows()):
                # Aquí 'row' corresponde a la fila actual del dataframe 'df'
                # Y 'row_no_estacional' corresponde a la fila actual del dataframe 'df_NoEstacional'
                
                valor_estacionalidad = row['BUENOS AIRES'] # Acceder al valor estacionalidad de 'df'

                fecha = row['Período']

                valor_sin_estacionalidad = row_no_estacional['BUENOS AIRES'] # Acceder al valor sin estacionalidad de 'df_NoEstacional'
                
                lista_fechas.append(fecha)

                lista_provincias.append(6)  # Carga de provincia POR ID - BUENOS AIRES: 6

                lista_valores_estacionalidad.append(valor_estacionalidad)  # Carga de valor POR ESTACIONALDIAD

                lista_valores_sin_estacionalidad.append(valor_sin_estacionalidad)
                
                lista_registro.append(1)  # Carga de tipo de registro - TIPO EMPLEO: 1
        

            df_aux['fecha'] = lista_fechas
            df_aux['id_prov'] = lista_provincias
            df_aux['tipo_registro'] = lista_registro
            df_aux['valores_estacionales'] = lista_valores_estacionalidad
            df_aux['valores_no_estacionales'] = lista_valores_sin_estacionalidad


            df_aux = df_aux.sort_values('fecha')

            print(df_aux)
  
        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")
    

LoadXLS5_1().loadInDataBase(file_path, host, user, password, database)

