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
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
    
        self.driver = webdriver.Chrome(options=options)

        # URL de la página que deseas obtener
        self.url_pagina = 'https://datos.produccion.gob.ar/dataset/puestos-de-trabajo-por-departamento-partido-y-sector-de-actividad'

    def descargar_archivo(self):
        print("Descarga de archivo de pagina web")
        # Desactivar advertencias de solicitud no segura
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Cargar la página web
        self.driver.get(self.url_pagina)

        # Esperar hasta que aparezca el enlace al primer archivo
        wait = WebDriverWait(self.driver, 10)
        archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/div/div/div/div[1]/div[3]/div[1]/div/a[2]")))

        # Obtener la URL del primer archivo
        url_archivo = archivo.get_attribute('href')

        # Esperar hasta que aparezca el enlace al segundo archivo
        archivo2 = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/div/div/div/div[1]/div[3]/div[3]/div/a[2]")))

        # Obtener la URL del segundo archivo
        url_archivo2 = archivo2.get_attribute('href')

        # Ruta de la carpeta donde guardar los archivos
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        # Nombre de los archivos
        nombre_archivo = 'trabajoSectorPrivado.csv'
        nombre_archivo2 = 'trabajoTotal.csv'
        
        # Descargar el primer archivo desactivando la verificación del certificado SSL
        response1 = requests.get(url_archivo, verify=False)
        ruta_guardado1 = os.path.join(carpeta_guardado, nombre_archivo)
        with open(ruta_guardado1, 'wb') as file1:
            file1.write(response1.content)
        print("Se descargo el archivo " + nombre_archivo)
        
        # Descargar el segundo archivo desactivando la verificación del certificado SSL
        response2 = requests.get(url_archivo2, verify=False)
        ruta_guardado2 = os.path.join(carpeta_guardado, nombre_archivo2)
        with open(ruta_guardado2, 'wb') as file2:
            file2.write(response2.content)
        print("Se descargo el archivo " + nombre_archivo2)
        # Cerrar el navegador
        self.driver.quit()