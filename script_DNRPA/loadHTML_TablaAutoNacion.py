import datetime
from bs4 import BeautifulSoup
import mysql.connector
import time
import numpy as np
import pandas as pd


host = 'localhost'
user = 'root'
password = 'Estadistica123'
database = 'prueba1'

class loadHTML_TablaAutoNacion:
    def loadInDataBase(self, host, user, password, database):
        # Se toma el tiempo de comienzo
        start_time = time.time()

        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        # Crear el cursor para ejecutar consultas
        cursor = conn.cursor()
        try:
            
            # Confirmar los cambios y cerrar el cursor y la conexión
            conn.commit()
            cursor.close()
            html_row = '''
            <tr>
                <th align="center"> Ene </th>
                <th align="center"> Feb </th>
                <th align="center"> Mar </th>
                <th align="center"> Abr </th>
                <th align="center"> May </th>
                <th align="center"> Jun </th>
                <th align="center"> Jul </th>
                <th align="center"> Ago </th>
                <th align="center"> Sep </th>
                <th align="center"> Oct </th>
                <th align="center"> Nov </th>
                <th align="center"> Dic </th>
                <th align="right"> Total </th>
            </tr>
            '''

            # Supongamos que tienes el HTML de la fila en una variable llamada 'html_row'
            soup = BeautifulSoup(html_row, 'html.parser')

            # Encuentra todas las etiquetas 'th' dentro de la fila
            cells = soup.find_all('th')

            # Crea un arreglo para almacenar los valores de las celdas
            values = []

            # Itera sobre cada celda y agrega su contenido al arreglo
            for cell in cells:
                values.append(cell.text.strip())

            # Imprime el arreglo de valores
            print(values)
            
            
            # Se toma el tiempo de finalización y se c  alcula
            end_time = time.time()
            duration = end_time - start_time
            print("-----------------------------------------------")
            print("Se guardaron los datos de SIPA NACIONAL CON ESTACIONALIDAD")
            print("Tiempo de ejecución:", duration)

            # Cerrar la conexión a la base de datos
            conn.close()

        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")
            conn.close()  # Cerrar la conexión en caso de error
            
loadHTML_TablaAutoNacion().loadInDataBase(host, user, password, database)