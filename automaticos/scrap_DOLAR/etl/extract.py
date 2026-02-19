"""
EXTRACT - Módulo de extracción de datos DOLAR
Responsabilidad: Extraer cotizaciones de 4 tipos de dólar desde dolarito.ar usando Selenium
"""
import logging
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)

URL = "https://www.dolarito.ar/"

# XPaths por tipo de dólar: (venta, compra)
XPATHS = {
    'oficial': (
        '/html/body/div[2]/div[2]/div[3]/ul/li[1]/div/div[2]/div[3]/div/div[1]/div/div[2]/div[2]/p',
        '/html/body/div[2]/div[2]/div[3]/ul/li[1]/div/div[2]/div[3]/div/div[2]/div/div[2]/div[2]/p',
    ),
    'blue': (
        '/html/body/div[2]/div[2]/div[3]/ul/li[3]/div/div[2]/div[3]/div/div[1]/div/div[2]/div[2]/p',
        '/html/body/div[2]/div[2]/div[3]/ul/li[2]/div/div[2]/div[3]/div/div[2]/div/div[2]/div[2]/p',
    ),
    'mep': (
        '/html/body/div[2]/div[2]/div[3]/ul/li[4]/div/div[2]/div[3]/div/div[1]/div/div[2]/div[2]/p',
        '/html/body/div[2]/div[2]/div[3]/ul/li[4]/div/div[2]/div[3]/div/div[2]/div/div[2]/div[2]/p',
    ),
    'ccl': (
        '/html/body/div[2]/div[2]/div[3]/ul/li[5]/div/div[2]/div[3]/div/div[1]/div/div[2]/div[2]/p',
        '/html/body/div[2]/div[2]/div[3]/ul/li[5]/div/div[2]/div[3]/div/div[2]/div/div[2]/div[2]/p',
    ),
}


class ExtractDOLAR:
    """Extrae las cotizaciones de 4 tipos de dólar desde dolarito.ar."""

    def extract(self) -> dict:
        """
        Extrae los 4 tipos de dólar y retorna un dict con DataFrames.

        Returns:
            dict: {'oficial': df, 'blue': df, 'mep': df, 'ccl': df}
        """
        logger.info("[EXTRACT] Iniciando extracción de cotizaciones desde %s", URL)
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        driver = webdriver.Chrome(options=options)

        resultados = {}
        try:
            driver.get(URL)
            driver.implicitly_wait(20)
            fecha_actual = datetime.now().strftime("%d/%m/%Y")

            for tipo, (xpath_venta, xpath_compra) in XPATHS.items():
                try:
                    venta  = driver.find_element(By.XPATH, xpath_venta).text
                    compra = driver.find_element(By.XPATH, xpath_compra).text
                    df = pd.DataFrame({'fecha': [fecha_actual], 'compra': [compra], 'venta': [venta]})
                    df['fecha']  = pd.to_datetime(df['fecha'], format="%d/%m/%Y")
                    df['compra'] = pd.to_numeric(df['compra'].str.replace('.', '').str.replace(',', '.'), errors='coerce')
                    df['venta']  = pd.to_numeric(df['venta'].str.replace('.', '').str.replace(',', '.'), errors='coerce')
                    resultados[tipo] = df
                    logger.info("[EXTRACT] %s — compra: %s | venta: %s", tipo, compra, venta)
                except Exception as e:
                    logger.warning("[EXTRACT] Error extrayendo dólar %s: %s", tipo, e)
        finally:
            driver.quit()

        return resultados
