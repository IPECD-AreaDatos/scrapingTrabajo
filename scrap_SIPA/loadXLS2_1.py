import datetime
import mysql.connector
import time
import pandas as pd

host = 'localhost'
user = 'root'
password = 'Estadistica123'
database = 'prueba1'

file_path = "C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scrap_SIPA\\files\\SIPA.xls"

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
            df = pd.read_excel(file_path, sheet_name=3)
            filas_total = len(df)
            # df.drop([0,1], axis=0, inplace=True)
            print("Longitud ---->", filas_total)
            fila = df.iloc[135]
            print("Fila: ", fila)

            # Reemplazar comas por puntos en los valores numéricos
            df = df.replace(',', '.', regex=True)

            # Obtener las columnas de interés
            fechas = df.iloc[:, 0]  # Primera columna (fechas)
            datos = df.iloc[:, 1:-1]  # Columnas restantes sin incluir la última columna

            # Convertir las fechas a objetos datetime
            fechas = pd.to_datetime(fechas, format="%b-%y*", errors='coerce')

            # Filtrar las fechas no válidas (NaT)
            fechas = fechas.dropna()

            # Convertir las fechas al formato deseado para la base de datos
            fechas_db = fechas.dt.strftime('%Y-%m-%d').tolist()

            print("Fechas: ", fechas_db)

            # Preparar la consulta de inserción
            for i in fechas_db:
                if i not in existing_dates:
                    print("fecha---->", i, "Existente----->", existing_dates)
                    insert_query = f"INSERT INTO {table_name} (Fecha) VALUES (%s)"
                    cursor.execute(insert_query, (i,))

                    
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
            
LoadXLS2_1().loadInDataBase(file_path, host, user, password, database)
   