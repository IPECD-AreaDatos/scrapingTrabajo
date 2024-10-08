import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os


class HomePage:
    
    def __init__(self):
        self.driver = webdriver.Chrome()

        # URL de la página que deseas obtener
        self.url_pagina = 'https://datos.produccion.gob.ar/dataset/salarios-por-departamento-partido-y-sector-de-actividad'

    def descargar_archivo(self):
        #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓PRIMER ARCHIVO↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
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
        archivo_SP = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/div/div/div/div[1]/div[3]/div[3]/div/a[2]")))

        # Obtener la URL del primer archivo
        url_archivo_SP = archivo_SP.get_attribute('href')

        # Nombre del primer archivo
        nombre_archivo_SP = 'salarioPromedioSP.csv'

        # Descargar el primer archivo
        response_1 = requests.get(url_archivo_SP, verify=False)

        # Guardar el primer archivo en la carpeta especificada
        ruta_guardado_1 = os.path.join(carpeta_guardado, nombre_archivo_SP)
        with open(ruta_guardado_1, 'wb') as file:
            file.write(response_1.content)
        
        #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓SEGUNDO ARCHIVO↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
        archivo_Total = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/div/div/div/div[1]/div[3]/div[5]/div/a[2]")))

        # Obtener la URL del segundo archivo
        url_archivo_Total = archivo_Total.get_attribute('href')

        # Nombre del segundo archivo
        nombre_archivo_Total = 'salarioPromedioTotal.csv'

        # Descargar el segundo archivo desactivando la verificación del certificado SSL
        response_2 = requests.get(url_archivo_Total, verify=False)

        # Guardar el segundo archivo en la carpeta especificada
        ruta_guardado_2 = os.path.join(carpeta_guardado, nombre_archivo_Total)
        with open(ruta_guardado_2, 'wb') as file:
            file.write(response_2.content)

        # Cerrar el navegador
        self.driver.quit()