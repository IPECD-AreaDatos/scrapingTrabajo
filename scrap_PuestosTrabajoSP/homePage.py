import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import os
import urllib3

class HomePage:
    
    def __init__(self):
        # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
        self.driver = webdriver.Chrome()  # Reemplaza con la ubicación de tu ChromeDriver

        # URL de la página que deseas obtener
        self.url_pagina = 'https://datos.produccion.gob.ar/dataset/puestos-de-trabajo-por-departamento-partido-y-sector-de-actividad'

    def descargar_archivo(self):
        # Desactivar advertencias de solicitud no segura
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Cargar la página web
        self.driver.get(self.url_pagina)

        # Esperar hasta que aparezca el enlace al archivo
        wait = WebDriverWait(self.driver, 10)
        archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/div/div/div/div[1]/div[3]/div[1]/div/a[2]")))

        # Obtener la URL del archivo
        url_archivo = archivo.get_attribute('href')

        # Ruta de la carpeta donde guardar el archivo
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        # Nombre del archivo
        nombre_archivo = 'trabajoSectorPrivado.csv'

        # Descargar el archivo desactivando la verificación del certificado SSL
        response = requests.get(url_archivo, verify=False)

        # Guardar el archivo en la carpeta especificada
        ruta_guardado = os.path.join(carpeta_guardado, nombre_archivo)
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)

        # Cerrar el navegador
        self.driver.quit()