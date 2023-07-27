import datetime
import time
import xlrd
import os
import pandas as pd
import mysql.connector

class LoadBuenosAires:
    
    def loadInDataBase(self):
        # Se toma el tiempo de comienzo
        start_time = time.time()

        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path = os.path.join(ruta_carpeta_files, 'BuenosAires.xls')
        
        
        try:
            
            
            # Leer el archivo de xls y obtener la hoja de trabajo específica
            workbook = xlrd.open_workbook(file_path)
            sheet = workbook.sheet_by_index(0)  # Hoja 1 (índice 0)


            # Definir el índice de la fila de fechas
            fecha_row_index = 4  # El índice de la fila que contiene las fechas (por ejemplo, línea 5)
            # Definir el índice de la primera fila de datos
            data_start_row_index = 10  # El índice de la primera fila que contiene los datos (por ejemplo, línea 11)
            
            # Obtener los años desde la fila de fechas, ignorando las celdas vacías o no numéricas
            anios = [int(anio) for anio in sheet.row_values(fecha_row_index)[1:] if isinstance(anio, (int, float))]

            # Crear listas para almacenar los valores por año y localidad
            valores_por_anio_y_localidad = []

            # Iterar por cada fila desde la primera fila de datos
            for row_index in range(data_start_row_index, min(sheet.nrows, 147)):
                localidad = sheet.cell_value(row_index, 0).strip()
                valores_por_localidad = sheet.row_values(row_index)[1:]
                for i, valor in enumerate(valores_por_localidad):
                    if isinstance(valor, (int, float)):
                        anio = anios[i]
                        fecha = datetime.date(year=anio, month=1, day=1)
                        valores_por_anio_y_localidad.append({
                            "Anio": fecha,
                            "Provincia": 6,
                            "Localidad": localidad,
                            "Valor": valor
                        })

            # Imprimir la lista completa de valores con año y localidad
            provincia = "Buenos Aires"
            print("Se estan cargando los valores de ", provincia)
            for item in valores_por_anio_y_localidad:
                print(item)

            print(f"Datos de {provincia} se leyeron correctamente")
            
            host = '172.17.22.10'
            user = 'Ivan'
            password = 'Estadistica123'
            database = 'prueba1'
            
            conexion = mysql.connector.connect(
                host=host,
                user=user,
                password=password,
                database=database
            )

            # Crear un cursor para ejecutar consultas
            cursor = conexion.cursor()

            # Consulta SQL para insertar los datos en la tabla
            insert_query = "INSERT INTO censo_provincia (Fecha, ID_Provincia, Departamentos, Poblacion) VALUES (%s, %s, %s, %s)"
            
            # Iterar por los datos y ejecutar la consulta para cada uno
            for item in valores_por_anio_y_localidad:
                fecha = item["Anio"].strftime('%Y-%m-%d')
                id_provincia = 1  # Aquí debes asignar el ID correspondiente a la provincia
                departamentos = item["Localidad"]
                poblacion = item["Valor"]

                # Ejecutar la consulta con los valores correspondientes
                cursor.execute(insert_query, (fecha, id_provincia, departamentos, poblacion))


            # Hacer commit para guardar los cambios en la base de datos
            conexion.commit()

            # Cerrar el cursor y la conexión
            cursor.close()
            conexion.close()
            
        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")
            
            
            
LoadBuenosAires().loadInDataBase()