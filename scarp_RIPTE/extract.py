import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os


class HomePage:
    
    def __init__(self):
        # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
        self.driver = webdriver.Chrome()  # Reemplaza con la ubicación de tu ChromeDriver

        # URL de la página que deseas obtener
        self.url_pagina = 'https://datos.gob.ar/vi/dataset/sspm-remuneracion-imponible-promedio-trabajadores-estables-ripte'

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
        archivo_SP = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/div/div/div/div[1]/div[3]/div/div/a[2]")))

        # Obtener la URL del primer archivo
        url_archivo_SP = archivo_SP.get_attribute('href')

        # Nombre del primer archivo
        nombre_archivo_SP = 'ripte_historico.csv'

        # Descargar el primer archivo
        response_1 = requests.get(url_archivo_SP, verify=False)

        # Guardar el primer archivo en la carpeta especificada
        ruta_guardado_1 = os.path.join(carpeta_guardado, nombre_archivo_SP)
        with open(ruta_guardado_1, 'wb') as file:
            file.write(response_1.content)


    #Objetivo: extraer el ultimo dato de ripte de la pagina de seguridad social. Luego el valor se reporta
    def extract_last_date(self):

        #Carga de pagina
        driver = webdriver.Chrome()
        driver.get('https://www.argentina.gob.ar/trabajo/seguridadsocial/ripte')
       
       #Buscamos la tabla que contiene los datos
        elemento = driver.find_element(By.XPATH, '//*[@id="block-system-main"]/section/article/div/div[9]/div/div/div/div/div[1]/div/h3')
        contenido_texto = elemento.text
        contenido_numerico = contenido_texto.replace('$', '').replace('.','').replace(',', '.')

        try:
            contenido_float = float(contenido_numerico)
            print("Contenido como float:", contenido_float)
        except ValueError:
            print("El contenido no es un número válido.")

        return contenido_numerico
