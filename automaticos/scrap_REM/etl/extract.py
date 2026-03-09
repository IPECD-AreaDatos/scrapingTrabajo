"""
EXTRACT - Módulo de extracción de datos REM
Responsabilidad: Descargar los archivos Excel del REM desde BCRA usando Selenium
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
URL = 'https://www.bcra.gob.ar/PublicacionesEstadisticas/Relevamiento_Expectativas_de_Mercado.asp'
XPATH_ULTIMO   = "/html/body/div/div[3]/div/div[1]/div[2]/p[2]/a[2]"
XPATH_HISTORICO = "/html/body/div/div[3]/div/div[1]/div[2]/p[7]/a"


class ExtractREM:
    """Descarga los archivos Excel del REM (último + histórico) desde BCRA."""

    def extract(self) -> tuple:
        """
        Descarga ambos archivos y retorna sus rutas locales.

        Returns:
            tuple: (ruta_ultimo, ruta_historico)
        """
        os.makedirs(FILES_DIR, exist_ok=True)
        driver = self._crear_driver()
        try:
            logger.info("[EXTRACT] Navegando a %s", URL)
            driver.get(URL)
            wait = WebDriverWait(driver, 10)

            link_ultimo = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_ULTIMO)))
            url_ultimo = link_ultimo.get_attribute('href')

            link_hist = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_HISTORICO)))
            url_hist = link_hist.get_attribute('href')
        finally:
            driver.quit()

        ruta_ultimo = self._descargar(url_ultimo, 'relevamiento_expectativas_mercado.xlsx')
        ruta_hist   = self._descargar(url_hist,   'historico_rem.xlsx')
        return ruta_ultimo, ruta_hist

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
        
        return driver