#Archivo destinado a descargar el archivo correspondiente a datos del IPC DE CABA
#FUENTE DE DATOS: https://www.estadisticaciudad.gob.ar/eyc/?p=128827

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import requests
import xlrd
import urllib3

class HomePage:
    
    def __init__(self):

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


        #Opciones de encabezado
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')

        #Instancia de navegador - Usamos Google Chrome
        self.driver = webdriver.Chrome(options=options)

        # URL de la fuente de datos
        self.url_pagina = 'https://www.estadisticaciudad.gob.ar/eyc/?p=128827'


    def descargar_archivo(self):
        
        # Cargar la página web
        self.driver.get(self.url_pagina)    

        #Tiempo de espera para cargar los elementos
        wait = WebDriverWait(self.driver, 10)
    
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')
        print(carpeta_guardado)

        # Crear la carpeta "files" si no existe
        if not os.path.exists(carpeta_guardado):
            os.makedirs(carpeta_guardado)
            
        #Esperar hasta que aparezca el link de descarga del archivo -- Boton con leyenda: "Ver archivo"
        archivo_ipc_caba = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div/div[2]/div/div/div[1]/main/article/div/a")))

        #Rescatamos el atributo "href" para luego descargar el archivo
        atributo_href = archivo_ipc_caba.get_attribute("href")

        #Obtenemos el archivo con un nombre asignado
        nombre_archivo = "ipc_caba.xlsx"
        
        response = requests.get(atributo_href, verify=False)

        # Guardar el primer archivo en la carpeta especificada
        ruta_guardado = os.path.join(carpeta_guardado, nombre_archivo)
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)
