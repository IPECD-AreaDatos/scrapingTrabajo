import pandas as pd
import os

class Transformer:

    #Definicion de atributos - Momentaneamente no es necesario
    def __init__(self):
        pass

    #Objetivo: construir el DF correspondiente a los valores del EMAE
    def construir_df_emae_valores(self):
        
        #Creamos direcciones para acceder al archivo
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')

        # Construir las rutas de los archivos
        file_path = os.path.join(ruta_carpeta_files, 'emae.xls')

        #Leemos archivos
        df = pd.read_excel(file_path, sheet_name=0, skiprows=4)
        df = df.drop(df.index[-1])  # Eliminar la última fila que contiene "Fuente: INDEC"
        
        
        """
        El archivo consiste en varias columnas, cada columna representa un valor en una fecha.
        Para la BDD el formato ideal es (FECHA,TIPO DE CATEGORIA[ID],VALOR).
        Por ende los pasos a ejecutar son:
        
        1 - Crear las fechas hasta el ultimo valor conocido
        2 - Recorrer columna por columna y generar esas series de valores (FECHA,TIPO DE CATEGORIA[ID],VALOR)
        3 - Ir concatenando cada conjunto de datos construido (los del paso anterior)
    
        """

        #PASO 1 - Creamos las fechas. La fecha inicial es ENERO DEL 2004
        num_rows = df.shape[0]

        # Generar un rango de fechas que comience en enero de 2004 y avance mensualmente
        fechas = pd.date_range(start='2004-01-01', periods=num_rows, freq='MS')


        # ========= #

        #PASO 2 - Generar las tuplas de valores (FECHA,TIPO DE CATEGORIA[ID],VALOR)

        # Eliminar las primeras dos columnas usando iloc - La primera Erepresenta la columna donde dice el año, y la segunda el mes.
        df = df.iloc[:, 2:]
        
        #Generamos un DATAFRAME acumulador - Contendra los datos que iremos concatenando
        df_acum = pd.DataFrame()

        
        #Lista de columnas que usaremos para recorrer el df
        list_columns = df.columns

        #En este for el INDEX representara el ID de la categoria al que pertenece el valor del EMAE.
        for index,column in enumerate(list_columns):
            
            #DF auxiliar que usamos para ordenar los datos
            df_aux = pd.DataFrame()
            
            df_aux['fecha'] = fechas #--> Asignamos las fechas
            df_aux['sector_productivo'] = index + 1 #--> Sumamos 1, ya que el index arranca de 0, y nuestros ID arrancan en 1.
            df_aux['valor'] = df[column]

            
            #PASO 3 - IR concatenando los DATAFRAME 
            df_acum = pd.concat([df_acum,df_aux])


        #Finalmente reorganizamos el dataframe con fines de manipularlo mejor en la carga
        df_acum = df_acum.sort_values(['fecha','sector_productivo'])


        #Retornamos el DF acum que contiene los datos ordenados.
        return df_acum

    #Objetivo: Construir el DF de las variaciones del EMAE
    def construir_df_emae_variaciones(self):
        
        #Creamos direcciones para acceder al archivo
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')

        # Construir las rutas de los archivos
        file_path = os.path.join(ruta_carpeta_files, 'emaevar.xls')

        # Leer el archivo Excel en un DataFrame de pandas
        df = pd.read_excel(file_path, sheet_name=0, skiprows=4, usecols="D,F")  # Leer el archivo XLSX y crear el DataFrame
        df = df.fillna(0) #--> Eliminamos todas las filas con valores vacios. Los que tengan 0 los dejamos.

        #Eliminar las ultimas 2 filas, porque contienen valores 0.
        df = df.drop(df.index[-1])
        df = df.drop(df.index[-1])

        """
        La lectura de este excel consiste en:
        1 - Cambiar nombres de columnas del DF
        2 - Generar fechas, teniendo como base FEBRERO DEL 2004.
        3 - Asignar las fechas al DF
        """

        # ==== PASO 1 - Cambiar nombres de columnas del DF
        df.columns = ['variacion_interanual','variacion_mensual']


        # ==== PASO 2 - GENERACION DE FECHAS
        num_rows = df.shape[0]

        # Generar un rango de fechas que comience en FEBRERO de 2004 y avance mensualmente
        fechas = pd.date_range(start='2004-02-01', periods=num_rows, freq='MS')

        # ==== PASO 2 - Asignacion de fechas al DF
        df['fecha'] = fechas
        
        #Reorganizamos el DF
        # Mover la columna 'fechas' al principio
        columns = ['fecha'] + [col for col in df.columns if col != 'fecha']
        df = df[columns]

        #Retornamos el DF de las variaciones.
        return df


