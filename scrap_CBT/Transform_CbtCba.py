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
    def readData(self):
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'CBT.xls')
        file_path_estimaciones_nea = os.path.join(ruta_carpeta_files, 'historico_estimaciones_nea.xlsx')
        file_path_destino = os.path.join(ruta_carpeta_files, 'Calculos.xlsx')
        

        """ 
        Leer el archivo Excel y seleccionar las columnas 1, 2 y 4 desde la fila 8

        En este caso l oque hacemos es, 
        1 - Leer datos del Excel
        2- A leer se toman algunos valores como nombres para las columnas
        3- Arreglamos el indice, ya que se rompe
        4 - Sort_index() resetea el indice
        """

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

        

        # ESTIMACIONES, VARIACIONES Y CALCULOS, ALMACENAR EN EL DATAWAREHOUSE
        df_final = self.calcular_estimaciones(concatenacion_df)

        print(df_final)
        # Cargar el archivo Excel existente
        excel_file = file_path_destino
        sheet_name = 'Hoja1'

        with pd.ExcelWriter(excel_file, mode='a', engine='openpyxl', if_sheet_exists='overlay') as writer:
            df_final.to_excel(writer, sheet_name=sheet_name, index=False, startrow=0, startcol=0)
           


    

    def calcular_estimaciones(self,concatenacion_df):
        
        

        concatenacion_df['Fecha'] = pd.to_datetime(concatenacion_df['Fecha'])


        # ==== CALCULOS DEL CBA DEL NEA ==== #

        #Valores para julio, donde se produce el cambio de periodo (es decir que para julio se usan datos del periodo anterior, este periodo es de 6 meses)
        lista_valores_periodo_anterior_cba_gba = concatenacion_df['CBA_Adulto'][(concatenacion_df['Fecha'].dt.year == 2022) & (concatenacion_df['Fecha'].dt.month >= 7)]
        lista_valores_periodo_anterior_cba_nea = concatenacion_df['cba_nea'][(concatenacion_df['Fecha'].dt.year == 2022) & (concatenacion_df['Fecha'].dt.month >= 7)]

        #Suma de listados conseguidos
        suma_cba_gba = sum(lista_valores_periodo_anterior_cba_gba)
        suma_cba_nea = sum(lista_valores_periodo_anterior_cba_nea)
        
        valor_julio_cba_gba = concatenacion_df['CBA_Adulto'][(concatenacion_df['Fecha'].dt.year == 2023) & (concatenacion_df['Fecha'].dt.month == 7)] #--> Buscamos el valor de julio de CBA

        valor_julio_cba_nea = (suma_cba_nea / suma_cba_gba) * valor_julio_cba_gba #--> Estimacion del cba del nea


        #=== Calculos del CBA en el ultimo periodo

        #Ultima fecha del dataset
        fecha_maxima = concatenacion_df['Fecha'].max()
        ultimo_año = fecha_maxima.year

        #Conseguimos los ultimos valores oficiales correspondientes al NEA de CBA
        lista_cba_nea = (concatenacion_df['cba_nea'][concatenacion_df['Fecha'].dt.year == ultimo_año].dropna())[-6:]
        suma_cba_nea = sum(lista_cba_nea)
 

        #Conseguimos los ultimos valores que le siguen a Julio y la lista de valores del ultimo periodo
        lista_ultimos_valores_cba = list(concatenacion_df['CBA_Adulto'][(concatenacion_df['Fecha'].dt.year == ultimo_año) & (concatenacion_df['Fecha'].dt.month >= 8)]) #--> Buscamos el valor de julio de CBA
        suma_cba_gba = sum((concatenacion_df['CBA_Adulto'][concatenacion_df['Fecha'].dt.year == ultimo_año].dropna())[:6])

        lista_ultimos_valores_cba_nea = [valor_julio_cba_nea]

        for i in lista_ultimos_valores_cba:

            estimacion_gba = (suma_cba_nea / suma_cba_gba ) * i
            lista_ultimos_valores_cba_nea.append(estimacion_gba)


    #Valores para julio, donde se produce el cambio de periodo (es decir que para julio se usan datos del periodo anterior, este periodo es de 6 meses)
        lista_valores_periodo_anterior_cbt_gba = concatenacion_df['CBT_Adulto'][(concatenacion_df['Fecha'].dt.year == 2022) & (concatenacion_df['Fecha'].dt.month >= 7)]
        lista_valores_periodo_anterior_cbt_nea = concatenacion_df['cbt_nea'][(concatenacion_df['Fecha'].dt.year == 2022) & (concatenacion_df['Fecha'].dt.month >= 7)]

        #Suma de listados conseguidos
        suma_cbt_gba = sum(lista_valores_periodo_anterior_cbt_gba)
        suma_cbt_nea = sum(lista_valores_periodo_anterior_cbt_nea)
        
        valor_julio_cbt_gba = concatenacion_df['CBT_Adulto'][(concatenacion_df['Fecha'].dt.year == 2023) & (concatenacion_df['Fecha'].dt.month == 7)] #--> Buscamos el valor de julio de CBA

        valor_julio_cbt_nea = (suma_cbt_gba / suma_cbt_nea) * valor_julio_cbt_gba #--> Estimacion del cba del nea


        #=== Calculos del CBT en el ultimo periodo

        #Ultima fecha del dataset
        fecha_maxima = concatenacion_df['Fecha'].max()
        ultimo_año = fecha_maxima.year

        #Conseguimos los ultimos valores oficiales correspondientes al NEA de CBA
        lista_cbt_nea = (concatenacion_df['cbt_nea'][concatenacion_df['Fecha'].dt.year == ultimo_año].dropna())[-6:]
        suma_cbt_nea = sum(lista_cbt_nea)
 

        #Conseguimos los ultimos valores que le siguen a Julio y la lista de valores del ultimo periodo
        lista_ultimos_valores_cbt = list(concatenacion_df['CBT_Adulto'][(concatenacion_df['Fecha'].dt.year == ultimo_año) & (concatenacion_df['Fecha'].dt.month >= 8)]) #--> Buscamos el valor de julio de CBA
        suma_cbt_gba = sum((concatenacion_df['CBT_Adulto'][concatenacion_df['Fecha'].dt.year == ultimo_año].dropna())[:6])

        lista_ultimos_valores_cbt_nea = [valor_julio_cbt_nea]

        for i in lista_ultimos_valores_cbt:

            estimacion_gba = (suma_cbt_nea / suma_cbt_gba ) * i
            lista_ultimos_valores_cbt_nea.append(estimacion_gba)



        tamaño_lista_cba = len(lista_ultimos_valores_cba_nea)


        # Obtiene el índice de las últimas 4 filas
        ultimos_indices = concatenacion_df.index[-int(tamaño_lista_cba):]
        
        for lugar_a_insertar,valor_cba,valor_cbt in zip(ultimos_indices,lista_ultimos_valores_cba_nea,lista_ultimos_valores_cbt_nea):
            
            concatenacion_df['cba_nea'].loc[lugar_a_insertar] = valor_cba
            concatenacion_df['cbt_nea'].loc[lugar_a_insertar] = valor_cbt


        concatenacion_df = concatenacion_df.sort_index() 

        return concatenacion_df
    
    
    #Esta funcion la usaremos para transformar y concatenar los datos que corresponde al datalake
    def transform_datalake(self):

        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'CBT.xls')
        file_path_estimaciones_nea = os.path.join(ruta_carpeta_files, 'historico_estimaciones_nea.xlsx')
        

        """ 
        Leer el archivo Excel y seleccionar las columnas 1, 2 y 4 desde la fila 8

        En este caso l oque hacemos es, 
        1 - Leer datos del Excel
        2- A leer se toman algunos valores como nombres para las columnas
        3- Arreglamos el indice, ya que se rompe
        4 - Sort_index() resetea el indice
        """

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

        
        return concatenacion_df

