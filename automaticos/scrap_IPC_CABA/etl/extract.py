"""
EXTRACT - Módulo de extracción de datos IPC CABA
Responsabilidad: Descargar el archivo Excel desde estadisticaciudad.gob.ar usando Selenium
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
NOMBRE_ARCHIVO = 'ipc_caba.xlsx'
URL = 'https://www.estadisticaciudad.gob.ar/eyc/?p=128827'
XPATH_LINK = "/html/body/div[1]/div[2]/div/div/div[1]/main/article/div/a"


class ExtractIPCCABA:
    """Descarga el archivo Excel del IPC CABA usando Selenium."""

    def extract(self) -> str:
        """
        Descarga el archivo y retorna la ruta local del archivo guardado.

        Returns:
            str: Ruta absoluta al archivo descargado
        """
        os.makedirs(FILES_DIR, exist_ok=True)
        driver = self._crear_driver()
        try:
            logger.info("[EXTRACT] Navegando a %s", URL)
            driver.get(URL)
            wait = WebDriverWait(driver, 10)
            link = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_LINK)))
            href = link.get_attribute("href")
            logger.info("[EXTRACT] URL del archivo: %s", href)
        finally:
            driver.quit()

        ruta = os.path.join(FILES_DIR, NOMBRE_ARCHIVO)
        response = requests.get(href, verify=False, timeout=30)
        response.raise_for_status()
        with open(ruta, 'wb') as f:
            f.write(response.content)
        logger.info("[EXTRACT] Archivo guardado en: %s", ruta)
        return ruta

    @staticmethod
    def _crear_driver():
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        return webdriver.Chrome(options=options)
