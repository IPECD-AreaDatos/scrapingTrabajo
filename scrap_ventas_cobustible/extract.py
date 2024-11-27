from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import requests
import logging

class Extraccion:
    
    def __init__(self):
        # Configurar las opciones de Chrome para ejecución en segundo plano
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # Modo sin cabeza (sin interfaz gráfica)
        options.add_argument('--disable-gpu')  # Deshabilitar GPU, útil para headless en algunos sistemas
        
        # Inicializar el navegador
        self.driver = webdriver.Chrome(options=options)

        # URL de la página que contiene los datos
        self.url_pagina = 'https://datos.gob.ar/dataset/energia-refinacion-comercializacion-petroleo-gas-derivados-tablas-dinamicas/archivo/energia_f0e4e10a-e4b8-44e6-bd16-763a43742107'

        # Configuración de logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def descargar_archivo(self):
        try:
            # Cargar la página web
            self.driver.get(self.url_pagina)
            self.logger.info(f"Accediendo a la página: {self.url_pagina}")
            
            # Esperar hasta que el enlace del archivo esté disponible
            wait = WebDriverWait(self.driver, 10)
            archivo_element = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/div/div/div[3]/a[1]")))
            
            # Obtener la URL del archivo
            url_archivo = archivo_element.get_attribute('href')
            self.logger.info(f"URL del archivo encontrada: {url_archivo}")
            
            # Configurar el directorio de descarga
            directorio_actual = os.path.dirname(os.path.abspath(__file__))
            carpeta_guardado = os.path.join(directorio_actual, 'files')

            # Crear la carpeta si no existe
            if not os.path.exists(carpeta_guardado):
                os.makedirs(carpeta_guardado)
                self.logger.info(f"Carpeta 'files' creada en: {carpeta_guardado}")

            # Nombre del archivo descargado
            nombre_archivo = 'ventas_combustible.csv'
            ruta_guardado = os.path.join(carpeta_guardado, nombre_archivo)

            # Descargar el archivo
            response = requests.get(url_archivo, verify=False)
            response.raise_for_status()  # Lanza una excepción si la descarga falla
            self.logger.info(f"Descargando archivo a: {ruta_guardado}")
            
            # Guardar el archivo en el directorio especificado
            with open(ruta_guardado, 'wb') as file:
                file.write(response.content)
            self.logger.info(f"Archivo guardado correctamente en: {ruta_guardado}")

        except Exception as e:
            self.logger.error(f"Error al descargar el archivo: {str(e)}")
        
        finally:
            # Cerrar el navegador después de completar la operación
            self.driver.quit()
            self.logger.info("Cerrando el navegador Selenium.")

