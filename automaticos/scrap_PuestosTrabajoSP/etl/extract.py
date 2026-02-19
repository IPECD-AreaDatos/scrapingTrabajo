"""
EXTRACT - Módulo de extracción de datos PuestosTrabajoSP
Responsabilidad: Descargar 2 CSVs (sector privado y total) desde datos.produccion.gob.ar
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
URL = 'https://datos.produccion.gob.ar/dataset/puestos-de-trabajo-por-departamento-partido-y-sector-de-actividad'
XPATH_PRIVADO = "/html/body/div[1]/div[2]/div/div/div/div[1]/div[3]/div[1]/div/a[2]"
XPATH_TOTAL   = "/html/body/div[1]/div[2]/div/div/div/div[1]/div[3]/div[3]/div/a[2]"


class ExtractPuestosTrabajoSP:
    """Descarga los 2 CSVs de puestos de trabajo."""

    def extract(self) -> tuple:
        """
        Descarga los 2 CSVs y retorna sus rutas locales.

        Returns:
            tuple: (ruta_privado, ruta_total)
        """
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
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

            elem1 = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_PRIVADO)))
            url1  = elem1.get_attribute('href')
            elem2 = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_TOTAL)))
            url2  = elem2.get_attribute('href')
        finally:
            driver.quit()

        ruta1 = os.path.join(FILES_DIR, 'trabajoSectorPrivado.csv')
        ruta2 = os.path.join(FILES_DIR, 'trabajoTotal.csv')

        for url, ruta, nombre in [(url1, ruta1, 'privado'), (url2, ruta2, 'total')]:
            resp = requests.get(url, verify=False, timeout=120)
            resp.raise_for_status()
            with open(ruta, 'wb') as f:
                f.write(resp.content)
            logger.info("[EXTRACT] CSV '%s' guardado: %s", nombre, ruta)

        return ruta1, ruta2
