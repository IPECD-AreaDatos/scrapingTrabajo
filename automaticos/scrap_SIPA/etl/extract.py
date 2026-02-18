"""
EXTRACT - Módulo de extracción de datos SIPA
Responsabilidad: Descargar el Excel de SIPA desde argentina.gob.ar usando Selenium
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

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')
URL = 'https://www.argentina.gob.ar/trabajo/estadisticas'
XPATH_LINK = "/html/body/main/div[2]/div/section[2]/div/div[9]/div/div/table/tbody/tr[1]/td[4]/a"
NOMBRE_ARCHIVO = 'SIPA.xlsx'


class ExtractSIPA:
    """Descarga el Excel de SIPA desde argentina.gob.ar."""

    def extract(self) -> str:
        """
        Descarga el Excel y retorna su ruta local.

        Returns:
            str: Ruta absoluta al archivo descargado
        """
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        os.makedirs(FILES_DIR, exist_ok=True)

        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=options)

        try:
            logger.info("[EXTRACT] Navegando a %s", URL)
            driver.get(URL)
            wait = WebDriverWait(driver, 10)
            elem = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_LINK)))
            url_archivo = elem.get_attribute('href')
            logger.info("[EXTRACT] URL del Excel: %s", url_archivo)
        finally:
            driver.quit()

        ruta = os.path.join(FILES_DIR, NOMBRE_ARCHIVO)
        resp = requests.get(url_archivo, timeout=120)
        resp.raise_for_status()
        with open(ruta, 'wb') as f:
            f.write(resp.content)
        logger.info("[EXTRACT] Excel guardado: %s", ruta)
        return ruta
