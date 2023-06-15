import datetime
import mysql.connector
import time
import numpy as np
import pandas as pd

host = 'localhost'
user = 'root'
password = 'Estadistica123'
database = 'prueba1'

file_path = "C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scrap_SIPA\\files\\SIPA.xlsx"

class LoadXLS2_1:
    def loadInDataBase(self, file_path, host, user, password, database):
        # Se toma el tiempo de comienzo
        start_time = time.time()

        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        # Crear el cursor para ejecutar consultas
        cursor = conn.cursor()
        try:
            # Nombre de la tabla en MySQL
            table_name = "sipa_nacional"
            
            # Obtener las fechas existentes en la tabla sipa_nacional
            select_dates_query = "SELECT Fecha FROM sipa_nacional"
            cursor.execute(select_dates_query)
            existing_dates = [row[0] for row in cursor.fetchall()]

            # Leer el archivo Excel en un DataFrame de pandas
            df = pd.read_excel(file_path)  # Leer el archivo XLSX y crear el DataFrame
            df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

            # Reemplazar comas por puntos en los valores numéricos
            df = df.replace(',', '.', regex=True)   
            
            fila = df.iloc[4]
            print("Fila ", fila)
            
            # Se toma el tiempo de finalización y se c  alcula
            end_time = time.time()
            duration = end_time - start_time
            print("-----------------------------------------------")
            print("Se guardaron los datos de IPC de la Region de Cuyo")
            print("Tiempo de ejecución:", duration)

            # Cerrar la conexión a la base de datos
            conn.close()

        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")
            conn.close()  # Cerrar la conexión en caso de error
            
LoadXLS2_1().loadInDataBase(file_path, host, user, password, database)
   