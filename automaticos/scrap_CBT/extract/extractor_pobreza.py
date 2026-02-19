import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import urllib3
import time


class ExtractorPobreza:
    """
    Extractor para descargar el archivo de Pobreza desde INDEC.
    
    Descarga el archivo cuadros_informe_pobreza.xls desde la página oficial del INDEC.
    """
    
    def __init__(self):
        # Configuración del navegador
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')

        self.driver = webdriver.Chrome(options=options)
        self.url_pagina = 'https://www.indec.gob.ar/indec/web/Nivel4-Tema-4-46-152'
    
    def descargar_archivo(self):
        """
        Descarga el archivo Pobreza.xls desde INDEC.
        
        Returns:
            str: Ruta del archivo descargado
        """
        # Desactivar advertencias de solicitud no segura
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Cargar la página web
        self.driver.get(self.url_pagina)

        wait = WebDriverWait(self.driver, 15)
        
        # Esperar a que la página cargue completamente
        time.sleep(5)
        
        # Crear carpeta de guardado si no existe
        directorio_actual = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        carpeta_guardado = os.path.join(directorio_actual, 'files', 'data')
        os.makedirs(carpeta_guardado, exist_ok=True)
            
        # Encontrar el enlace al archivo usando un selector más robusto
        try:
            # Intentar encontrar el enlace por el href que contiene "cuadros_informe_pobreza"
            archivo = wait.until(EC.presence_of_element_located(
                (By.XPATH, "//a[contains(@href, 'cuadros_informe_pobreza') and contains(@href, '.xls')]")
            ))
        except:
            # Si falla, intentar con el XPath absoluto original
            archivo = wait.until(EC.presence_of_element_located(
                (By.XPATH, "/html/body/div[2]/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[2]/div/div[2]/div/div[2]/div/div/a")
            ))
        
        # Obtener la URL del archivo
        url_archivo = archivo.get_attribute('href')
        print(f"[EXTRACT] URL del archivo Pobreza: {url_archivo}")
        
        # Descargar el archivo
        nombre_archivo = 'Pobreza.xls'
        ruta_guardado = os.path.join(carpeta_guardado, nombre_archivo)
        
        response = requests.get(url_archivo)
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)

        print(f"[EXTRACT] Archivo guardado en: {ruta_guardado}")
        self.driver.quit()
        
        return ruta_guardado
