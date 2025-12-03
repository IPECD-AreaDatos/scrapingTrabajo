"""
EXTRACT - Módulo de extracción de datos IPC (INDEC)
Responsabilidad: Descargar archivos Excel desde la web del INDEC
"""
import os
import time
import logging
import requests
from urllib3 import disable_warnings
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Ignoramos advertencias SSL de requests
disable_warnings()

logger = logging.getLogger(__name__)

class ExtractIPC:
    """Clase para extraer datos del IPC mediante Selenium"""
    
    def __init__(self, headless=True):
        self.url_pagina = 'https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-5-31'
        self.directorio_actual = os.path.dirname(os.path.abspath(__file__))
        self.directorio_proyecto = os.path.dirname(self.directorio_actual) # carpeta scrap_IPC
        self.carpeta_files = os.path.join(self.directorio_proyecto, 'files')
        self.headless = headless
        self.driver = None

    def extract(self):
        """
        Ejecuta la descarga de los 3 archivos del IPC.
        Returns:
            dict: Diccionario con las rutas de los archivos descargados.
        """
        try:
            if not os.path.exists(self.carpeta_files):
                os.makedirs(self.carpeta_files)
                logger.info(f"Carpeta creada: {self.carpeta_files}")

            self._iniciar_driver()
            
            logger.info(f"Accediendo a: {self.url_pagina}")
            self.driver.get(self.url_pagina)
            wait = WebDriverWait(self.driver, 20)

            rutas_descargadas = {}

            # 1. Archivo Nacional (Mes/Año)
            logger.info("Buscando archivo Nacional (sh_ipc_mes_ano)...")
            xpath_nac = "/html/body/div[2]/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[2]/div/div[2]/div[1]/div[2]/div/div/a"
            rutas_descargadas['nacional'] = self._descargar_archivo(xpath_nac, 'sh_ipc_mes_ano.xls', wait)

            # 2. Archivo Aperturas (Regiones)
            logger.info("Buscando archivo Aperturas (sh_ipc_aperturas)...")
            xpath_aperturas = "/html/body/div[2]/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[2]/div/div[2]/div[3]/div[2]/div/div/a"
            rutas_descargadas['aperturas'] = self._descargar_archivo(xpath_aperturas, 'sh_ipc_aperturas.xls', wait)

            # 3. Archivo Precios Promedio
            logger.info("Buscando archivo Precios Promedio...")
            xpath_precios = "/html/body/div[2]/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[2]/div/div[2]/div[5]/div[2]/div/div/a"
            rutas_descargadas['precios'] = self._descargar_archivo(xpath_precios, 'sh_ipc_precios_promedio.xls', wait)

            logger.info("[OK] Extracción completada.")
            return rutas_descargadas

        except Exception as e:
            logger.error(f"Error en extracción IPC: {e}")
            raise
        finally:
            if self.driver:
                self.driver.quit()

    def _iniciar_driver(self):
        """Configura Selenium para Linux/Headless"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36')
            chrome_options.add_argument('--window-size=1920,1080')
        
        # Intentar encontrar chromedriver en rutas comunes
        rutas_posibles = [
            '/usr/bin/chromedriver',
            '/usr/local/bin/chromedriver',
            os.path.join(self.directorio_proyecto, 'selenium', 'chromedriver')
        ]
        
        service = None
        for ruta in rutas_posibles:
            if os.path.exists(ruta):
                service = Service(ruta)
                break
        
        if service:
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            self.driver = webdriver.Chrome(options=chrome_options)

    def _descargar_archivo(self, xpath, nombre_archivo, wait):
        """Helper para obtener URL y descargar con requests"""
        try:
            elemento = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            url = elemento.get_attribute('href')
            
            logger.info(f"Descargando {nombre_archivo} desde {url}...")
            
            ruta_destino = os.path.join(self.carpeta_files, nombre_archivo)
            
            response = requests.get(url, verify=False, timeout=60)
            if response.status_code == 200:
                with open(ruta_destino, 'wb') as f:
                    f.write(response.content)
                return ruta_destino
            else:
                raise Exception(f"Status code {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error descargando {nombre_archivo}: {e}")
            raise