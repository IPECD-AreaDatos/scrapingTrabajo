import datetime
import mysql.connector
import time
import pandas as pd


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
            df = pd.read_excel(file_path, sheet_name=3, skiprows=2)

            # Reemplazar comas por puntos en los valores numéricos
            df = df.replace(',', '.', regex=True)
            df = df.replace('*', '', regex=True)

            # Obtener las columnas de interés
            fechas = df.iloc[:134, 0]  # Primera columna (fechas)
            datos = df.iloc[:, 1:7]  # Columnas restantes sin incluir la última columna
            
            print("Fechas ", fechas)

            # Se toma el tiempo de finalización y se calcula
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