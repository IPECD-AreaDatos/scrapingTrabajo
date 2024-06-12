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
    
        #Desactivamos protecciones 
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
    
        # Configuración del navegador
        driver = webdriver.Chrome(options=options)

        # URL de la página que deseas obtener
        url_pagina = 'https://www.argentina.gob.ar/trabajo/estadisticas'

        # Cargar la página web
        driver.get(url_pagina)

        wait = WebDriverWait(driver, 10)

        # Encontrar el enlace al archivo
        archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/main/div[2]/div/section[2]/div/div[9]/div/div/table/tbody/tr[1]/td[4]/a")))
                                                                        
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
        nombre_archivo = 'SIPA.xlsx'

        # Descargar el archivo
        response = requests.get(url_archivo)

        # Guardar el archivo en la carpeta especificada
        ruta_guardado = f'{carpeta_guardado}\\{nombre_archivo}'
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)

        # Cerrar el navegador
        driver.quit()
