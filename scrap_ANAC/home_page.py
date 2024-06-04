import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os

class HomePage:
    
    def __init__(self):
        # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        self.driver = webdriver.Chrome(options=options)

        # URL de la página que deseas obtener
        self.url_pagina = 'https://datos.anac.gob.ar/estadisticas/'

    def descargar_archivo(self):
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
        archivo_anac = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/main/section[2]/div/div[2]/div[11]/a")))

        # Obtener la URL del primer archivo
        url_archivo_anac = archivo_anac.get_attribute('href')

        # Nombre del primer archivo
        nombre_archivo = 'anac.xlsx'

        # Descargar el primer archivo
        response_1 = requests.get(url_archivo_anac, verify=False)

        # Guardar el primer archivo en la carpeta especificada
        ruta_guardado_1 = os.path.join(carpeta_guardado, nombre_archivo)
        with open(ruta_guardado_1, 'wb') as file:
            file.write(response_1.content)
