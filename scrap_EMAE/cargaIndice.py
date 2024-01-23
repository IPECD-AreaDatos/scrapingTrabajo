import numpy as np
import pandas as pd
from datetime import datetime
import mysql
import mysql.connector
from dateutil.relativedelta import relativedelta
from informes import InformesEmae

class cargaIndice:

    def __init__(self):

        self.conn = None
        self.cursor = None 

    def conecta_bdd(self,host, user, password, database):

        # Conexión a la base de datos MySQL
        self.conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        self.cursor = self.conn.cursor()


        
    def loadXLSIndiceEMAE(self, file_path, lista_fechas, lista_SectorProductivo, lista_valores, host, user, password, database):



        # Leer el archivo Excel en un DataFrame de pandas
        df = pd.read_excel(file_path, sheet_name=0, skiprows=1)  # Leer el archivo XLSX y crear el DataFrame
        df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None
        # Eliminar la última fila que contiene "Fuente: INDEC"
        df = df.drop(df.index[-1])
        df = df.drop(df.index[-1])

        # Obtener las columnas desde C hasta R
        columnas_valores = df.columns[2:18]  # Columnas C a R

        lista_columnas = list(columnas_valores)

        #Lista de valores, contiene numeros del 1 al 16. Representan las categorias del EMAE.
        rango_columnas = range(1,len(columnas_valores) + 1)
   
        fecha_inicio = datetime(2003, 12, 1)
        num_meses = len(df) - 2  # Restar 2 para compensar las filas de encabezados

        lista_fechas = [fecha_inicio + relativedelta(months=i) for i in range(num_meses)]

        # Iterar a través de las filas a partir de la fila 3
        for index, row in df.iterrows():
            if index >= 2:  # Fila 3 en adelante
                fecha = lista_fechas[index - 2]  # Restar 2 para compensar el índice
                for columna in columnas_valores:

                    #Buscamos el valor por FILA|COLUMNA y lo agregamos a la lista
                    valor = df.at[index, columna]
                    lista_valores.append(valor)
                    
                    #Buscamos el sector por FILA|COLUMNA y lo agregamos a la lista
                
                    sector_productivo = df.at[0, columna]  # Fila 1 (Fila de los años)

                    indice_sector_productivo = lista_columnas.index(columna) + 1

                    lista_SectorProductivo.append(indice_sector_productivo)
                    print(f"Fecha: {fecha}, Valor: {valor}, Sector Productivo: {indice_sector_productivo}")
        
        

        #Conectamos a la BDD 
        self.conecta_bdd(host, user, password, database)

        #Verificar cantidad de filas anteriores 
        select_row_count_query = "SELECT COUNT(*) FROM emae"
        self.cursor.execute(select_row_count_query)
        row_count_before = self.cursor.fetchone()[0]
        
        delete_query ="TRUNCATE `ipecd_economico`.`emae`"
        self.cursor.execute(delete_query)
        
        # Iterar a través de las filas a partir de la fila 3
        for index, row in df.iterrows():
            if index >= 3:  # Fila 3 en adelante
                fecha = lista_fechas[index - 2]  # Restar 2 para compensar el índice
                for columna in columnas_valores:

                    #Buscamos el valor por FILA|COLUMNA y lo agregamos a la lista
                    valor = df.at[index, columna]
                    indice_sector_productivo = lista_columnas.index(columna) + 1

                    # Insertar en la tabla MySQL
                    query = "INSERT INTO emae (Fecha, Sector_Productivo, Valor) VALUES (%s, %s, %s)"
                    values = (fecha, indice_sector_productivo, valor)
                    self.cursor.execute(query, values)

        self.conn.commit()

        
        self.cursor.execute(select_row_count_query)
        row_count_after = self.cursor.fetchone()[0]
        #Comparar la cantidad de antes y despues
        
        if row_count_after > row_count_before:
            print("Se agregaron nuevos datos")
            InformesEmae(host, user, password, database).enviar_mensajes()
        else:
            print("Se realizo una verificacion de la base de datos")
            
        
        self.conn.commit()
        # Cerrar la conexión a la base de datos
        self.cursor.close()
        self.conn.close()
            


