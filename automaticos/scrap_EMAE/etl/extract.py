"""
EXTRACT - Módulo de extracción de datos EMAE
Responsabilidad: Descargar los 2 XLS del EMAE desde INDEC usando Selenium
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
URL = 'https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-9-48'
XPATH_VALORES     = "/html/body/div[2]/div[1]/div[2]/div[4]/div[1]/div[2]/div/div/div/div/a[2]"
XPATH_VARIACIONES = "/html/body/div[2]/div[1]/div[2]/div[4]/div[1]/div[2]/div/div/div/div/a[1]"


class ExtractEMAE:
    """Descarga los 2 XLS del EMAE (valores y variaciones) desde INDEC."""

    def extract(self) -> tuple:
        """
        Descarga ambos archivos y retorna sus rutas locales.

        Returns:
            tuple: (ruta_valores, ruta_variaciones)
        """
        os.makedirs(FILES_DIR, exist_ok=True)
        driver = self._crear_driver()
        try:
            logger.info("[EXTRACT] Navegando a %s", URL)
            driver.get(URL)
            wait = WebDriverWait(driver, 10)

            link_val = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_VALORES)))
            url_val = link_val.get_attribute('href')

            link_var = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_VARIACIONES)))
            url_var = link_var.get_attribute('href')
        finally:
            driver.quit()

        ruta_val = self._descargar(url_val, 'emae.xls')
        ruta_var = self._descargar(url_var, 'emaevar.xls')
        return ruta_val, ruta_var

    def _descargar(self, url: str, nombre: str) -> str:
        ruta = os.path.join(FILES_DIR, nombre)
        logger.info("[EXTRACT] Descargando %s...", nombre)
        response = requests.get(url, verify=False, timeout=60)
        response.raise_for_status()
        with open(ruta, 'wb') as f:
            f.write(response.content)
        logger.info("[EXTRACT] Guardado en: %s", ruta)
        return ruta

    @staticmethod
    def _crear_driver():
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        return webdriver.Chrome(options=options)
