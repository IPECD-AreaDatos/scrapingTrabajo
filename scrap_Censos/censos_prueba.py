import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import os
import urllib3
import pandas as pd
import xlrd
from bs4 import BeautifulSoup
import datetime
import mysql.connector

class homePage:
    def construir_df_estimaciones(self):
        
        
            
            # Establecer la conexión a la base de datos MySQL
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

            # Variables para transacciones
            transaccion_activa = False

            # Iterar por los datos y ejecutar la consulta para cada uno
            for item in valores_por_anio_y_localidad:
                fecha = item["Anio"].strftime('%Y-%m-%d')
                codigo_provincia = item["Provincia"]  # Cambia el nombre de la variable
                departamentos = item["Localidad"]
                poblacion = item["Valor"]

                # Ejecutar la consulta con los valores correspondientes
                cursor.execute(insert_query, (fecha, codigo_provincia, departamentos, poblacion))


            # Hacer commit para guardar los cambios en la base de datos
            conexion.commit()

            # Cerrar el cursor y la conexión
            cursor.close()
            conexion.close()
            

    def imprimir_rutas(self):
            
       print(self.lista_rutas)


instancia = homePage()

instancia.descargar_archivos()
instancia.construir_df_estimaciones()