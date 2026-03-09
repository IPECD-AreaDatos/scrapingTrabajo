"""
EXTRACT - Módulo de extracción de datos SalarioMVM
Responsabilidad: Descargar el CSV de salario mínimo vital y móvil desde datos.gob.ar
"""
import os
import logging
import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')
URL = 'https://datos.gob.ar/dataset/sspm-salario-minimo-vital-movil-pesos-corrientes/archivo/sspm_57.1'
XPATH_LINK = '/html/body/div[1]/div[2]/div/div/div[3]/a[1]'
NOMBRE_ARCHIVO = 'salario_minimo.csv'


class ExtractSalarioMVM:
    """Descarga el CSV del Salario Mínimo Vital y Móvil."""

    def extract(self) -> str:
        """
        Descarga el CSV y retorna su ruta local.

        Returns:
            str: Ruta absoluta al archivo descargado
        """
        os.makedirs(FILES_DIR, exist_ok=True)

        options = webdriver.ChromeOptions()
        options.add_argument('--headless') # Mantenemos el headless pero con disfraz
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--window-size=1920,1080')
        
        # EL DISFRAZ CLAVE:
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        driver = webdriver.Chrome(options=options)
        
        # Eliminar el rastro de 'navigator.webdriver'
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })

        try:
            logger.info("[EXTRACT] Navegando a %s", URL)
            driver.get(URL)
            time.sleep(10)  # La página requiere tiempo para cargar el link
            link = driver.find_element(By.XPATH, XPATH_LINK)
            href = link.get_attribute('href')
            logger.info("[EXTRACT] URL del CSV: %s", href)
        finally:
            driver.quit()

        ruta = os.path.join(FILES_DIR, NOMBRE_ARCHIVO)
        response = requests.get(href, timeout=60)
        response.raise_for_status()
        with open(ruta, 'wb') as f:
            f.write(response.content)
        logger.info("[EXTRACT] CSV guardado en: %s", ruta)
        return ruta
