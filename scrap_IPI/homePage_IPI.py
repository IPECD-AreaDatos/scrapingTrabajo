import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import urllib3

class HomePage_IPI:

    #Inicializacion de atributos
    def __init__(self):

        pass


    def descargar_archivo(self):
        
        #=== CONFIGURACION DEL NAVEGADOR

        #Desactivamos advertencias 
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        #Agregamos opcion de "sin vista"
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
    
        # Configuración del navegador
        driver = webdriver.Chrome(options=options)

        #=== OBTENCION DEL ARCHIVO

        # URL de IPI nacion - 
        url_pagina = 'https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-6-14'

        # Cargar la página web
        driver.get(url_pagina)
        wait = WebDriverWait(driver, 10)


        """
        Se extrae el atributo hred de una elemento HTML. En esta pagina existe solo un 
        archivo, de nombre: 
        *  "Índice de producción industrial manufacturero (IPI manufacturero). 
            Nivel general, divisiones y subclases. Serie original,
            desestacionalizada y tendencia-ciclo, base 2004=100."
        """
        archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div/a")))

        # Obtener la URL del archivo
        url_archivo = archivo.get_attribute('href')


        #Finalmente, se descarga el archivo
        response = requests.get(url_archivo)


        # ==== GUARDAMOS EL ARCHIVO

        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')


        # Crear la carpeta "files" si no existe
        if not os.path.exists(carpeta_guardado):
            os.makedirs(carpeta_guardado)


        #Definimos un nombre para el archivo
        nombre_archivo = 'IPI.xls'


        # Guardar el archivo en la carpeta especificada
        ruta_guardado = os.path.join(carpeta_guardado, nombre_archivo)

        ruta_guardado = os.path.normpath(ruta_guardado)

        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)

        # Cerrar el navegador
        driver.quit()


