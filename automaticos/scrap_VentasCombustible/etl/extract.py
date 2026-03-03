"""
EXTRACT - Módulo de extracción de datos VentasCombustible
Responsabilidad: Descargar el CSV de ventas de combustible desde datos.gob.ar usando Selenium
"""
import os
import logging
import requests
import time
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

    def _crear_driver(self):
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

    def extract(self) -> str:
        os.makedirs(FILES_DIR, exist_ok=True)
        ruta = os.path.join(FILES_DIR, NOMBRE_ARCHIVO)

        # --- NUEVO: Verificación de frescura del archivo ---
        if os.path.exists(ruta):
            tiempo_archivo = os.path.getmtime(ruta)
            # 3600 segundos = 1 hora
            if (time.time() - tiempo_archivo) < 3600:
                logger.info("[EXTRACT] Usando archivo existente (descargado hace menos de 1 hora).")
                return ruta
        # ----------------------------------------------------
        
        # 1. Obtener URL con Selenium
        driver = self._crear_driver()
        try:
            logger.info("[EXTRACT] Navegando a %s", URL)
            driver.get(URL)
            wait = WebDriverWait(driver, 10)
            elem = wait.until(EC.presence_of_element_located((By.XPATH, XPATH_LINK)))
            url_archivo = elem.get_attribute('href')
            logger.info("[EXTRACT] URL del CSV: %s", url_archivo)
        finally:
            driver.quit()

        # 2. Descarga por Streaming con Reintentos (Solución al IncompleteRead)
        max_reintentos = 3
        for i in range(max_reintentos):
            try:
                logger.info(f"[EXTRACT] Intentando descarga (Intento {i+1})...")
                with requests.get(url_archivo, stream=True, timeout=300, verify=False) as r:
                    r.raise_for_status()
                    with open(ruta, 'wb') as f:
                        for chunk in r.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                logger.info("[EXTRACT] Descarga completada.")
                return ruta
            except requests.exceptions.RequestException as e:
                logger.warning(f"[EXTRACT] Fallo en intento {i+1}: {e}")
                time.sleep(5)
                if i == max_reintentos - 1:
                    raise
