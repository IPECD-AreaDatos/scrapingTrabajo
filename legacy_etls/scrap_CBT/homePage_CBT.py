import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import os
import urllib3
import time

class HomePageCBT:
    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')

        self.driver = webdriver.Chrome(options=options)
        self.url_pagina = 'https://www.indec.gob.ar/indec/web/Nivel4-Tema-4-43-149'

    def descargar_archivo(self):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.driver.get(self.url_pagina)
        wait = WebDriverWait(self.driver, 15)

        # Esperar a que el botón "Canasta básica" sea clickeable y hacer clic
        boton = wait.until(EC.element_to_be_clickable((
            By.XPATH, "//div[contains(text(), 'Canasta básica')]"
        )))
        boton.click()

        # Esperar a que el div con id=1 se haga visible
        wait.until(EC.visibility_of_element_located((By.ID, "1")))

        # Ahora encontrar el enlace al archivo .xls
        archivo = wait.until(EC.presence_of_element_located(
            (By.XPATH, "//a[contains(@href, 'serie_cba_cbt.xls')]")
        ))

        url_archivo = archivo.get_attribute('href')
        print(f"URL del archivo: {url_archivo}")

        # Crear carpeta de guardado si no existe
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        carpeta_guardado = os.path.join(directorio_actual, 'files')
        os.makedirs(carpeta_guardado, exist_ok=True)

        # Descargar el archivo
        nombre_archivo = 'CBT.xls'
        ruta_guardado = os.path.join(carpeta_guardado, nombre_archivo)

        response = requests.get(url_archivo)
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)

        print(f"Archivo guardado en: {ruta_guardado}")
        self.driver.quit()
