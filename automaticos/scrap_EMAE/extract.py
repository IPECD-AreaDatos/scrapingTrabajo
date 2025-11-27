import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import os
import urllib3
import subprocess


class HomePage:
    def descargar_archivos(self):

        #Configuracion del navegador
        options = webdriver.ChromeOptions()

        # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
        driver = webdriver.Chrome(options=options)
        
        #=== PRIMER ARCHIVO - VALORES DE EMAE
        # URL de la página que deseas obtener
        url_pagina = 'https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-9-48'

        # Cargar la página web
        driver.get(url_pagina)

        #Esperamos 10 segundos
        wait = WebDriverWait(driver, 10)

        # Encontrar el enlace al archivo
        archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[2]/div[4]/div[1]/div[2]/div/div/div/div/a[2]")))

        # Obtener la URL del archivo
        url_archivo = archivo.get_attribute('href')

        # Ruta de la carpeta donde guardar el archivo
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        # Crear la carpeta "files" si no existe
        if not os.path.exists(carpeta_guardado):
            os.makedirs(carpeta_guardado)

        # Nombre del archivo
        nombre_archivo = 'emae.xls'

        # Descargar el archivo
        response = requests.get(url_archivo)

        # Guardar el archivo en la carpeta especificada
        ruta_guardado = os.path.join(carpeta_guardado, nombre_archivo)
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)
            
        
        #=== SEGUNDO ARCHIVO - EMAR VARIACIONES
        # Encontrar el enlace al segundo archivo
        archivo_2 = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[2]/div[4]/div[1]/div[2]/div/div/div/div/a[1]")))
        # Obtener la URL del segundo archivo
        url_archivo_2 = archivo_2.get_attribute('href')
        # Imprimir la URL del segundo archivo
        print(url_archivo_2)

        #Nombre del archivo de las variaciones
        nombre_archivo_2 = 'emaevar.xls' 

        #Descargamos el archivo de las variaciones
        response_2 = requests.get(url_archivo_2)

        # Guardar el segundo archivo en la carpeta especificada
        ruta_guardado_2 = os.path.join(carpeta_guardado, nombre_archivo_2)
        with open(ruta_guardado_2, 'wb') as file:
            file.write(response_2.content)

        # Cerrar el navegador
        driver.quit()