"""
EXTRACT - Módulo de extracción de datos VentasCombustible
Responsabilidad: Descargar el CSV de ventas de combustible desde datos.gob.ar usando Selenium
"""
import os
import logging
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')
URL = 'https://datos.gob.ar/dataset/energia-refinacion-comercializacion-petroleo-gas-derivados-tablas-dinamicas/archivo/energia_f0e4e10a-e4b8-44e6-bd16-763a43742107'
XPATH_LINK = "/html/body/div[1]/div[2]/div/div/div[3]/a[1]"
NOMBRE_ARCHIVO = 'ventas_combustible.csv'


class ExtractVentasCombustible:
    """Descarga el CSV de ventas de combustible."""

    def extract(self) -> str:
        """
        Descarga el CSV y retorna su ruta local.

        Returns:
            str: Ruta absoluta al archivo descargado
        """
        os.makedirs(FILES_DIR, exist_ok=True)
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(options=options)

        try:
            logger.info("[EXTRACT] Navegando a %s", URL)
            driver.get(URL)
            wait = WebDriverWait(driver, 10)
            elem = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_LINK)))
            url_archivo = elem.get_attribute('href')
            logger.info("[EXTRACT] URL del CSV: %s", url_archivo)
        finally:
            driver.quit()

        ruta = os.path.join(FILES_DIR, NOMBRE_ARCHIVO)
        resp = requests.get(url_archivo, verify=False, timeout=120)
        resp.raise_for_status()
        with open(ruta, 'wb') as f:
            f.write(resp.content)
        logger.info("[EXTRACT] CSV guardado: %s", ruta)
        return ruta
