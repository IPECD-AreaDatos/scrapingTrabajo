"""
En este script manejamos dos hojas del mismo excel,
"primera_hoja": corresponde a los datos de CBA y CBT de ADULTOS. Es decir, por individuo.
"segunda_hoja": corresponde a los datos de CBA y CBT de FAMILIAS. El tipo de familia es el 2, que es un grupo de 4 personas
"""

import datetime
import time
import os
from openpyxl import load_workbook
import pandas as pd

#Clase encargada de manejar y construir los excels.
#En el archivo "Calculos.xls" se almacenan los datos finales qe seran almacenados en la BDD.
class loadXLSDataCBT:    
    def transform_datalake(self):

        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'CBT.xls')
        file_path_estimaciones_nea = os.path.join(ruta_carpeta_files, 'historico_estimaciones_nea.xlsx')

        # Datos de la primera hoja
        df_primeraHoja = pd.read_excel(file_path_desagregado, sheet_name=0, usecols=[0, 1, 3], skiprows=7)
        valores_que_estan_como_columna = df_primeraHoja.columns.to_list()
        df_primeraHoja.loc[-1] = valores_que_estan_como_columna
        df_primeraHoja.index = df_primeraHoja.index + 1
        df_primeraHoja = df_primeraHoja.sort_index()
        df_primeraHoja.columns = ['Fecha','CBA_Adulto','CBT_Adulto']

        #Datos de la primera fila
        df_segundaHoja = pd.read_excel(file_path_desagregado, sheet_name=3, usecols=[2,6], skiprows=7)
        valores_que_estan_como_columna = df_segundaHoja.columns.to_list()
        df_segundaHoja.loc[-1] = valores_que_estan_como_columna
        df_segundaHoja.index = df_segundaHoja.index + 1
        df_segundaHoja = df_segundaHoja.sort_index()
        df_segundaHoja.columns = ['CBA_Hogar','CBT_Hogar']

        # Encuentra la fila en blanco y elimina las filas posteriores
        indice_fila_en_blanco = df_primeraHoja.index[df_primeraHoja.isnull().all(axis=1)].tolist()[0]
        df_primeraHoja = df_primeraHoja.iloc[:indice_fila_en_blanco]

        indice_fila_en_blanco = df_segundaHoja.index[df_segundaHoja.isnull().all(axis=1)].tolist()[0]
        df_segundaHoja = df_segundaHoja.iloc[:indice_fila_en_blanco]

        #Datos oficiales de CBA y CBT del NEA
        concatenacion_df = pd.read_excel(file_path_estimaciones_nea)
        concatenacion_df = concatenacion_df.drop('Fecha',axis = 1)

        #Transformacion de los ultimos datos --> Objetivo de datalake?
        concatenacion_df = pd.concat([df_primeraHoja,df_segundaHoja,concatenacion_df],axis=1)
        concatenacion_df['Fecha'] = pd.to_datetime(concatenacion_df['Fecha'])

        df_definitivo = self.estimaciones_nea(concatenacion_df)

        return df_definitivo
    

    def estimaciones_nea(self,concatenacion_df):

    
        #Establecemos la fecha del ultimo periodo valido (recodar que para estimaciones se usa los ultimos 6 valores oficiales del NEA)
        fecha_ultima_publicacion_oficial = pd.to_datetime("2024-06-01")

        #Verificacion para ver si es necesario o no calcular estimaciones del NEA - Se supone que el mismo mes que salen DATOS OFICIALES no se tiene que calcular
        if len(concatenacion_df['cba_nea'][concatenacion_df['Fecha'] > fecha_ultima_publicacion_oficial]) == 0:
            return concatenacion_df

        #En caso de que no estemos en el mismo mes de publicacion, se calculas las estimaciones teniendo en cuenta los ultimos datos oficiales del NEA
        else:
            #En base a la fecha buscamos los ultimos 6 valores de CBA y CBT DE GBA ,CBA y CBT de NEA
            df_sin_nulos = concatenacion_df[concatenacion_df['Fecha'] <= fecha_ultima_publicacion_oficial][-6:]

            #Sumamos los valores de los grupos que vamos a usar
            suma_cba = sum(df_sin_nulos['CBA_Adulto'])
            suma_cbt = sum(df_sin_nulos['CBT_Adulto'])
            suma_cba_nea =sum(df_sin_nulos['cba_nea'])
            suma_cbt_nea = sum(df_sin_nulos['cbt_nea'])
    

            #Insersion al dataframe de las estimaciones
            df_con_nulos = concatenacion_df[concatenacion_df['Fecha'] > fecha_ultima_publicacion_oficial]

            
            #Recorremos los datos nulos, calculamos sus estimaciones y finalmente los insertamos al DF definitivo
            for index,row in df_con_nulos.iterrows():
                
                #Calculos de las estimaciones
                estimacion_cba = row['CBA_Adulto'] * ( suma_cba_nea / suma_cba)
                estimacion_cbt = row['CBT_Adulto'] * (suma_cbt_nea / suma_cbt)

                concatenacion_df.loc[index, 'cba_nea'] = estimacion_cba
                concatenacion_df.loc[index, 'cbt_nea'] = estimacion_cbt


            return concatenacion_df
        

