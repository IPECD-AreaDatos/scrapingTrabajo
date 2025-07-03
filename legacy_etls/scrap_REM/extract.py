from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import requests
import xlrd

class Extract:
    
    #Valor de los atributos de la clase
    def __init__(self):


        options = webdriver.ChromeOptions()
        options.add_argument('--headless')

        #Instancia de navegador - Usamos Google Chrome
        self.driver = webdriver.Chrome(options=options)

        # URL de la página que deseas obtener
        self.url_pagina = 'https://www.bcra.gob.ar/PublicacionesEstadisticas/Relevamiento_Expectativas_de_Mercado.asp'


    #Objetivo: descargar los archivos que vamos a usar.
    def descargar_archivo(self):

        # ======= PRIMERA ZONA, DESCARGA DEL ULTIMO INFORME DEL REM ======= #

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
        
        #Esperamos que aparezca el elemento, esta dirreccion corresponde
        #al ultimo archivo publicado por el REM. 
        archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div/div[3]/div/div[1]/div[2]/p[2]/a[2]")))

        # Obtener la URL al ultimo archivo publicado por el REM.
        url_archivo = archivo.get_attribute('href')

        #Nombre del archivo del REM
        nombre_archivo = 'relevamiento_expectativas_mercado.xlsx'

        # Descargar el primer archivo
        response = requests.get(url_archivo, verify=False)

        # Guardar el primer archivo en la carpeta especificada
        ruta_guardado = os.path.join(carpeta_guardado, nombre_archivo)
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)

        # ======= SEGUNDA ZONA, DESCARGA DE LOS DATOS HISTORICOS DEL REM ======= #
    
        #Busqueda del link usando XPATH
        archivo_historico_rem = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div/div[3]/div/div[1]/div[2]/p[7]/a")))

        # Obtener la URL al alrchivo del historico del REM
        url_archivo_historico = archivo_historico_rem.get_attribute('href')

        # Descargar el archivo
        response = requests.get(url_archivo_historico, verify=False)

        #Nombre del archivo del Historico
        nombre_archivo_historico = 'historico_rem.xlsx'

        # Guardar el archivo de los datos historicos
        ruta_guardado = os.path.join(carpeta_guardado, nombre_archivo_historico)
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)


        