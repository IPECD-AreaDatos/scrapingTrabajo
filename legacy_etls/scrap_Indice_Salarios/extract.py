import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import urllib3

class HomePage:
    
    def __init__(self):
        #Desactivamos protecciones 
        #urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        #options = webdriver.ChromeOptions()
        #options.add_argument('--headless')
    
        # Configuración del navegador
        self.driver = webdriver.Chrome()

        # URL de la página que deseas obtener
        self.url_pagina = 'https://www.indec.gob.ar/indec/web/Nivel4-Tema-4-31-61'

    def descargar_archivo(self):
         # Cargar la página web
        self.driver.get(self.url_pagina)

        wait = WebDriverWait(self.driver, 10)
        
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        # Esperar hasta que aparezca el enlace al primer archivo
        archivo_SP = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[2]/div/div[2]/div[2]/div[2]/div/div/a")))

        # Obtener la URL del primer archivo
        url_archivo_SP = archivo_SP.get_attribute('href')

        # Nombre del primer archivo
        nombre_archivo_SP = 'indice_salarios.csv'

        # Descargar el primer archivo
        response_1 = requests.get(url_archivo_SP, verify=False)

        # Guardar el primer archivo en la carpeta especificada
        ruta_guardado_1 = os.path.join(carpeta_guardado, nombre_archivo_SP)
        with open(ruta_guardado_1, 'wb') as file:
            file.write(response_1.content)


