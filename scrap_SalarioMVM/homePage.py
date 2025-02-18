import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import pandas as pd
from datetime import datetime
import re
import time
from selenium import webdriver


class HomePage:
            
    def __init__(self):

        #Creamos instancia de Selenium
        self.driver = webdriver.Chrome()  

        # URL de la página que deseas obtener
        self.url_pagina = 'https://datos.gob.ar/dataset/sspm-salario-minimo-vital-movil-pesos-corrientes/archivo/sspm_57.1'

        self.directorio_actual = os.path.dirname(os.path.abspath(__file__)) #--> Directorio actual

        self.carpeta_guardado = os.path.join(self.directorio_actual, 'files')# --> combinamos "directorio_actual" con ruta de la carpeta "files"



    def descargar_archivo(self):

        #Cargamos la pagina
        self.driver.get(self.url_pagina)

        # Hacer que Selenium espere durante 5 segundos antes de continuar
        time.sleep(15)
        
        #Detectamos boton de descarga - Es una etiqueta <a>, y obtenemos el atributo HREF
        etiqueta_a_descarga = self.driver.find_element(By.XPATH,'/html/body/div[1]/div[2]/div/div/div[3]/a[1]')
        href = etiqueta_a_descarga.get_attribute('href')

        #Deteccion de direcciones de carpeta - queremos guardar el archivo en "files"
        nombre_archivo = "salario_minimo.csv"

        # Descargar el archivo
        response = requests.get(href)


        # Guardar el archivo en la carpeta especificada
        ruta_guardado = f'{self.carpeta_guardado}\\{nombre_archivo}'
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)


        
        self.driver.close()

    
    def tratamiento_df(self):

        nombre_archivo = "salario_minimo.csv"
        ruta_guardado = f'{self.carpeta_guardado}\\{nombre_archivo}'
        df = pd.read_csv(ruta_guardado)

        # === Transformar fechas === #
        fechas = df['indice_tiempo'] #--> Serie de fechas

        nuevas_fechas = [] #--> Lista de nuevas fechas


        for fecha in fechas:

            nueva_fecha = datetime.strptime(fecha,'%Y-%m-%d').date() #--> Transformamos a formato fecha - formato (DIA-MES-AÑO)
 
            nuevas_fechas.append(nueva_fecha)
            
        df['indice_tiempo'] = nuevas_fechas


        return df
