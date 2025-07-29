import os
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import logging
logger = logging.getLogger("emae") # Obtiene el logger ya configurado por run_emae

def extract_emae_data():
    logger.info("Iniciando extraccion de archivos EMAE con Selenium (headless)")

    # Configurar Selenium para modo headless en servidor Linux/EC2
    chrome_options = Options()
    #chrome_options.add_argument('--headless')  # necesario para EC2
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920,1080')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])  # sin devtools

    driver = webdriver.Chrome(options=chrome_options)

    try:
        url_pagina = 'https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-9-48'
        driver.get(url_pagina)

        # Scroll por si no carga en headless
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        wait = WebDriverWait(driver, 15)

        # Buscar los archivos
        logger.info("Localizando primer archivo (EMAE)...")
        archivo_1 = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[2]/div[4]/div[1]/div[2]/div/div/div/div/a[2]")))
        url_1 = archivo_1.get_attribute('href')

        logger.info("Localizando segundo archivo (Variaciones)...")
        archivo_2 = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[2]/div[4]/div[1]/div[2]/div/div/div/div/a[1]")))
        url_2 = archivo_2.get_attribute('href')

        if not url_1 or not url_2:
            raise Exception("No se encontraron las URLs de los archivos .xls")

        # Carpeta destino
        base_path = os.path.dirname(os.path.abspath(__file__))
        folder_dest = os.path.abspath(os.path.join(base_path, '../../data/raw/emae'))
        os.makedirs(folder_dest, exist_ok=True)

        archivos = [
            {"url": url_1, "filename": "emae.xls"},
            {"url": url_2, "filename": "emaevar.xls"},
        ]

        for archivo in archivos:
            ruta_final = os.path.join(folder_dest, archivo["filename"])
            logger.info(f"Descargando {archivo['filename']} desde {archivo['url']}...")
            response = requests.get(archivo["url"])
            with open(ruta_final, 'wb') as f:
                f.write(response.content)
            logger.info(f"{archivo['filename']} guardado en {ruta_final}")

        logger.info("Extraccion completada correctamente con Selenium.")

    except Exception as e:
         logger.info(f"Error en la extraccion con Selenium: {e}")

    finally:
        driver.quit()
