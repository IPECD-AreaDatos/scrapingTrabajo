import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import os
import urllib3
import time

class HomePagePobreza:
    def __init__(self):
        # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
        self.driver = webdriver.Chrome()  # Reemplaza con la ubicación de tu ChromeDriver

        # URL de la página que deseas obtener
        self.url_pagina = 'https://www.indec.gob.ar/indec/web/Nivel4-Tema-4-46-152'
    
    def descargar_archivo(self):
        # Desactivar advertencias de solicitud no segura
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Cargar la página web
        self.driver.get(self.url_pagina)

        wait = WebDriverWait(self.driver, 10)
        
        time.sleep(30)
        
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        # Crear la carpeta "files" si no existe
        if not os.path.exists(carpeta_guardado):
            os.makedirs(carpeta_guardado)
            
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
        nombre_archivo = 'Pobreza.xls'

        # Descargar el archivo
        response = requests.get(url_archivo)

        # Guardar el archivo en la carpeta especificada
        ruta_guardado = f'{carpeta_guardado}\\{nombre_archivo}'
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)

        # Cerrar el navegador
        self.driver.quit()