from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import requests
import time
import xlrd

class Extract:
    #Valor de los atributos de la clase
    def __init__(self):

        options = webdriver.ChromeOptions()
        options.add_argument('--headless')

        #Instancia de navegador - Usamos Google Chrome
        self.driver = webdriver.Chrome(options=options)

        # URL de la página que deseas obtener
        self.url_pagina = 'https://public.tableau.com/app/profile/estadisticas.public/viz/Descarga-CoberturayFinanciacinRev4/Descargar'

    #Objetivo: descargar los archivos que vamos a usar.
    def descargar_archivo(self):

        # ======= PRIMERA ZONA, DESCARGA DEL ULTIMO INFORME DEL REM ======= #

        # Cargar la página web
        self.driver.get(self.url_pagina)

        wait = WebDriverWait(self.driver, 20)
        
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        # Crear la carpeta "files" si no existe
        if not os.path.exists(carpeta_guardado):
            os.makedirs(carpeta_guardado)
        
        # Esperar a que cargue el iframe principal de Tableau
        iframe = wait.until(EC.presence_of_element_located((By.TAG_NAME, "iframe")))
        self.driver.switch_to.frame(iframe)

        # Hacer clic en el botón de descarga
        boton_descarga = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Descargar')]")))
        boton_descarga.click()

        # Esperar a que aparezca la opción de descargar archivos
        opcion_descargar_datos = wait.until(EC.element_to_be_clickable((By.XPATH, "//span[text()='Datos']")))
        opcion_descargar_datos.click()

        # Cambiar de nuevo al iframe de descarga que aparece
        self.driver.switch_to.default_content()
        iframe_descarga = wait.until(EC.presence_of_element_located((By.TAG_NAME, "iframe")))
        self.driver.switch_to.frame(iframe_descarga)

        # Seleccionar opción para descargar archivo completo
        descargar_todo = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Descargar archivo completo')]")))
        descargar_todo.click()

        # Esperar unos segundos para asegurar que se complete la descarga
        time.sleep(300)  # Ajusta el tiempo según sea necesario

        print("Archivo descargado con éxito en la carpeta 'files'.")

        # Cerrar el navegador
        self.driver.quit()