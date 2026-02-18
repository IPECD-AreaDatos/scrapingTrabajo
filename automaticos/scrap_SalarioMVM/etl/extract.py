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
        options.add_argument('--headless')
        driver = webdriver.Chrome(options=options)

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
