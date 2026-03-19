"""
EXTRACT - Módulo de extracción de datos Pobreza
Responsabilidad: Descargar el XLS de Pobreza e Indigencia desde INDEC usando Selenium
"""
import os
import logging
import requests
import urllib3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)

# Configuración de rutas y URL
FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files', 'data')
NOMBRE_ARCHIVO = 'Pobreza.xls'
URL = 'https://www.indec.gob.ar/indec/web/Nivel4-Tema-4-46-152'
# Full XPath proporcionado por el usuario
XPATH_LINK = "/html/body/div[2]/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[2]/div/div[2]/div[1]/div[2]/div/div/a"

class ExtractorPobreza:
    """Descarga el XLS de cuadros de informe de pobreza desde INDEC."""

    def descargar_archivo(self) -> str:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        os.makedirs(FILES_DIR, exist_ok=True)
        
        driver = self._crear_driver()
        try:
            logger.info("[EXTRACT-POBREZA] Navegando a %s", URL)
            driver.get(URL)
            
            wait = WebDriverWait(driver, 30)
            link_element = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_LINK)))
            href = link_element.get_attribute('href')
            logger.info("[EXTRACT-POBREZA] URL del archivo detectada: %s", href)
            
        except Exception as e:
            logger.error("[EXTRACT-POBREZA] Error buscando el link: %s", e)
            raise e
        finally:
            driver.quit()

        ruta = os.path.join(FILES_DIR, NOMBRE_ARCHIVO)
        response = requests.get(href, verify=False, timeout=60)
        response.raise_for_status()
        
        with open(ruta, 'wb') as f:
            f.write(response.content)
            
        logger.info("[EXTRACT-POBREZA] Archivo guardado en: %s", ruta)
        return ruta

    @staticmethod
    def _crear_driver():
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        driver = webdriver.Chrome(options=options)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
        return driver