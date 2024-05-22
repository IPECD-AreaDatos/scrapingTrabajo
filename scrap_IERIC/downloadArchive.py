import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import os
import urllib3

class downloadArchive:
    def __init__(self):

        options = webdriver.ChromeOptions()
        options.add_argument('--headless')

        # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
        self.driver = webdriver.Chrome(options=options)  # Reemplaza con la ubicación de tu ChromeDriver

        # URL de la página que deseas obtener
        self.url_pagina = 'https://www.ieric.org.ar/series_estadisticas/corrientes/'

    def descargar_archivo(self):

        # Desactivar advertencias de solicitud no segura
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Cargar la página web
        self.driver.get(self.url_pagina)

        wait = WebDriverWait(self.driver, 10)
        
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        # Crear la carpeta "files" si no existe
        if not os.path.exists(carpeta_guardado):
            os.makedirs(carpeta_guardado)

        # Esperar hasta que aparezca el enlace al primer archivo
        archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div/div[2]/div/div[1]/div[3]/div[2]/div[1]/div[1]/a")))

        # Obtener la URL del primer archivo
        url_archivo = archivo.get_attribute('href')

        # Descargar el primer archivo desactivando la verificación del certificado SSL
        response = requests.get(url_archivo, verify=False)
        ruta_guardado = os.path.join(carpeta_guardado, 'Empresas en actividad.xls')
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)