import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import pandas as pd
import xlrd
from datetime import date
from dateutil.relativedelta import relativedelta


class HomePage_IPI:

    
    def __init__(self):

        df = None #--> Dataframe que contiene los datos pedidos


    def descargar_archivo(self):
    
        # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
        driver = webdriver.Chrome()  # Reemplaza con la ubicación de tu ChromeDriver


        # URL de la página que deseas obtener
        url_pagina = 'https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-6-14'

        # Cargar la página web
        driver.get(url_pagina)
        wait = WebDriverWait(driver, 10)

        # Encontrar el enlace al archivo
        archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div/a")))

        # Obtener la URL del archivo
        url_archivo = archivo.get_attribute('href')
        # Imprimir la URL del archivo
        print(url_archivo)


        # Ruta de la carpeta donde guardar el archivo
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        # Nombre del archivo
        nombre_archivo = 'IPI.xls'


        # Descargar el archivo
        response = requests.get(url_archivo)

        # Guardar el archivo en la carpeta especificada
        ruta_guardado = f'{carpeta_guardado}/{nombre_archivo}'
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)

        # Cerrar el navegador
        driver.quit()



    def construir_df(self):


        # Ruta de la carpeta donde guardar el archivo
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files\\')

        # Nombre del archivo
        nombre_archivo = 'IPI.xls'

        #Construimos cadena final para buscar el archivo
        path_guardado = carpeta_guardado + nombre_archivo


        #Abrimos EXCEl, y ubicamos en la hoja la hoja que queremos
        woorkbook = xlrd.open_workbook(path_guardado)

        sheet = woorkbook.sheet_by_index(2)


        #Nombre de columnas
        nombre_cols = ['var_IPI','var_interanual_alimentos','var_interanual_textil','var_interanual_maderas','var_interanual_sustancias','var_interanual_MinNoMetalicos','var_interanual_metales']

        # Crear el dataframe - especificamos, HOJA - QUE COLUMNAS USAMOS - LOS NOMBRES DE LAS COLUMNAS -LA FILA DONDE ARRANCA
        self.df = pd.read_excel(path_guardado,sheet_name='Cuadro 3',usecols='D,E,V,AE,AO,BB,BM',names=nombre_cols,skiprows=16)
        self.df = self.df.dropna()

        self.df = self.df / 100

        #Generador de fechas
        fecha_inicio = date(2017, 1,1)
        num_meses = len(self.df)  # Restar 2 para compensar las filas de encabezados

        lista_fechas = [fecha_inicio + relativedelta(months=i) for i in range(num_meses)]


        self.df['fecha'] = lista_fechas

 

        return self.df



