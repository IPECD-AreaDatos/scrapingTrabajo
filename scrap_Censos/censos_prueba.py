import openpyxl
import datetime
import os
import mysql.connector
from itertools import zip_longest
import pandas as pd
from sqlalchemy import create_engine

class homePage:
    def construir_df_estimaciones(self, host, user, password, database):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path = os.path.join(ruta_carpeta_files, 'Estimación Modificada Censo 2022.xlsx')
        
        db_connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        
        # Cargar el archivo XLSX
        workbook = openpyxl.load_workbook(file_path)
        sheet = workbook.active
        # Obtener las fechas desde la fila 1, columna E hasta el final de la fila
        fecha_row_index = 1
        fechas_str = [cell.value for cell in sheet[fecha_row_index]][4:]
        fechas_date = [datetime.date(year=fecha, month=1, day=1) for fecha in fechas_str]

        # Inicializar listas para almacenar los datos
        data = {
            'Fecha': [],
            'ID_Provincia': [],
            'ID_Departamento': [],
            'Poblacion': []
        }

        # Iterar sobre las filas de datos
        for row in sheet.iter_rows(min_row=3, values_only=True):
            id_provincia = row[0]
            id_departamento = row[2]
            valores = row[4:]

            # Agregar los datos a las listas
            data['Fecha'].extend(fechas_date)
            data['ID_Provincia'].extend([id_provincia] * len(fechas_date))
            data['ID_Departamento'].extend([id_departamento] * len(fechas_date))
            data['Poblacion'].extend(valores)

        # Crear el DataFrame
        df = pd.DataFrame(data)
        return df
    


            
        

