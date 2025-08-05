import os
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib3
import time

def extract_anac_data():
    print("📥 Iniciando descarga del archivo ANAC desde la web...")

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(options=options)
    wait = WebDriverWait(driver, 15)

    try:
        driver.get('https://datos.anac.gob.ar/estadisticas/')
        time.sleep(2)

        # Paso 1: click en categoría
        print("🧭 Haciendo click en categoría...")
        categoria = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/main/section[2]/div/div[2]/div[12]/a/div')))
        categoria.click()

        # Paso 3: menú de descarga
        print("🧭 Abriendo menú de descarga...")
        menu_descarga = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/main/div/div[1]/div/table/tbody/tr[2]/td[2]/a/span[2]/a/span[1]')))
        menu_descarga.click()

        

        # Paso 2: buscar directamente el enlace al Excel
        print("🔍 Buscando enlace al archivo Excel...")
        enlace_excel = wait.until(EC.presence_of_element_located((By.XPATH, '/html/body/main/div/div[1]/div/table/tbody/tr[2]/td[2]/div/ul/li/a/span[2]')))
        url_archivo = enlace_excel.get_attribute('href')

        if not url_archivo.endswith(".xlsx"):
            raise Exception(f"No se detectó URL de archivo .xlsx. URL actual: {url_archivo}")

        print(f"✅ URL del archivo Excel: {url_archivo}")

        # Guardar archivo
        base_path = os.path.dirname(os.path.abspath(__file__))
        carpeta_files = os.path.abspath(os.path.join(base_path, '../../data/raw'))
        os.makedirs(carpeta_files, exist_ok=True)
        ruta_archivo = os.path.join(carpeta_files, 'ANAC.xlsx')

        response = requests.get(url_archivo)
        with open(ruta_archivo, 'wb') as f:
            f.write(response.content)

        print(f"✅ Archivo descargado en: {ruta_archivo}")
        return ruta_archivo

    except Exception as e:
        print(f"❌ Error durante la descarga: {e}")
        return None

    finally:
        driver.quit()