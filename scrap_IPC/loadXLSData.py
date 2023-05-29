import mysql.connector
import numpy as np
import pandas as pd
import time
import xlrd
from datetime import datetime, timedelta

class LoadXLSData:
    def loadInDataBase(self, file_path):
        #Se toma el tiempo de comienzo
        start_time = time.time()
        
        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='Estadistica123',
            database='prueba1'
        )

        try:
            # Nombre de la tabla en MySQL
            table_name = 'ipc_regionnea'

            # Leer el archivo de xls y obtener los datos de la línea específica en la hoja 3
            workbook = xlrd.open_workbook(file_path)
            sheet = workbook.sheet_by_index(2)  # Hoja 3 (índice 2)
            target_row_index = 154  # Índice de la línea específica que deseas obtener (por ejemplo, línea 3)

           # Obtener los valores de la fila completa a partir de la segunda columna (columna B)
            target_row_values = sheet.row_values(target_row_index, start_colx=1)  # start_colx=1 indica que se inicia desde la columna B

            # Convertir los valores de número de serie de fecha de Excel a formato de fecha
            for i in range(len(target_row_values)):
                 if isinstance(target_row_values[i], float):
                    excel_date = int(target_row_values[i])
                    dt = datetime(1899, 12, 30) + timedelta(days=excel_date)
                    target_row_values[i] = dt.strftime('%Y-%m-%d')
                    
            # Insertar los valores en la tabla de la base de datos
            insert_query = f"INSERT INTO {table_name} ({'Fecha'}) VALUES (%s)"

            for value in target_row_values:
                conn.cursor().execute(insert_query, (value,))

            conn.commit()

            print("GUARDO!!!!!")
            # Se toma el tiempo de finalización y se calcula
            end_time = time.time()
            duration = end_time - start_time
            print(f"Tiempo de ejecución: {duration} segundos")

            # Cerrar la conexión a la base de datos
            conn.close()

        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Ocurrió un error durante la carga de datos: {str(e)}")
            conn.close()  # Cerrar la conexión en caso de error