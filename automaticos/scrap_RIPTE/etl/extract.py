"""
EXTRACT - Módulo de extracción de datos RIPTE
Responsabilidad: Extraer el último valor de RIPTE desde argentina.gob.ar usando Selenium
"""
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)

URL = 'https://www.argentina.gob.ar/trabajo/seguridadsocial/ripte'
XPATH_VALOR = '/html/body/main/div[2]/div/section/article/div/div[9]/div/div/div/div/div[2]/div/div/span'


class ExtractRIPTE:
    """Extrae el último valor de RIPTE desde la web de Seguridad Social."""

    def extract(self) -> str:
        """
        Extrae el último valor numérico del RIPTE.

        Returns:
            str: Valor numérico del RIPTE como string
        """
        logger.info("[EXTRACT] Navegando a %s", URL)
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=options)

        try:
            driver.get(URL)
            elemento = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.XPATH, XPATH_VALOR))
            )
            texto = elemento.text
            valor = texto.replace('$', '').replace('.', '').replace(',', '.')
            logger.info("[EXTRACT] Último valor RIPTE: %s", valor)
            return valor
        finally:
            driver.quit()
