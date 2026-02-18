"""
EXTRACT - Módulo de extracción de datos IPC_Online
Responsabilidad: Extraer datos del IPC desde ipconlinebb.wordpress.com usando Selenium
"""
import re
import time
import logging
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)

URL = 'https://ipconlinebb.wordpress.com/'


class ExtractIPCOnline:
    """Extrae los datos del IPC Online desde WordPress."""

    def extract(self) -> pd.DataFrame:
        """
        Extrae variación mensual, interanual y acumulada del IPC.

        Returns:
            pd.DataFrame con columnas: fecha, variacion_mensual, variacion_interanual_acumulada, variacion_acumulada_del_año
        """
        logger.info("[EXTRACT] Navegando a %s", URL)
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=options)

        try:
            driver.set_page_load_timeout(30)
            driver.get(URL)
            time.sleep(5)

            # Variación mensual
            var_mensual_elem = driver.find_element(By.XPATH, '/html/body/div[1]/div/div/main/article[1]/div[1]/h2')
            texto_mensual = var_mensual_elem.text
            texto_mensual = re.sub(r'%|,', '', texto_mensual)
            var_mensual = float(texto_mensual) / 100

            # Variaciones adicionales
            variaciones_elem = driver.find_element(By.XPATH, '/html/body/div[1]/div/div/main/article[1]/div[1]/p[1]')
            numeros = re.findall(r'\d+,\d+', variaciones_elem.text)
            numeros_proc = [float(n.replace(',', '.')) for n in numeros]

            # Fecha
            fecha_elem = driver.find_element(By.XPATH, '/html/body/div[1]/div/div/main/article/header/h1')
            match = re.search(r'([A-Za-z]+) (\d{4})', fecha_elem.text)
            meses = {
                "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
                "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
            }
            nombre_mes = match.group(1).lower()
            year = int(match.group(2))
            fecha_obj = datetime(year, meses[nombre_mes], 1).date()

            df = pd.DataFrame({
                'fecha': [fecha_obj],
                'variacion_mensual': [var_mensual],
                'variacion_interanual_acumulada': [numeros_proc[1]],
                'variacion_acumulada_del_año': [numeros_proc[2]],
            })
            logger.info("[EXTRACT] IPC Online: fecha=%s | mensual=%.4f", fecha_obj, var_mensual)
            return df
        finally:
            driver.quit()
