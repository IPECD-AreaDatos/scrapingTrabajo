import os
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib3

def extract_anac_data():
    print("📥 Iniciando descarga del archivo ANAC desde la web...")

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(options=options)
    driver.get('https://datos.anac.gob.ar/estadisticas/')

    wait = WebDriverWait(driver, 10)
    archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/main/section[2]/div/div[2]/div[12]/a")))
    url_archivo = archivo.get_attribute('href')

    # Ruta donde guardar el archivo
    base_path = os.path.dirname(os.path.abspath(__file__))
    carpeta_files = os.path.abspath(os.path.join(base_path, '../../data/raw'))
    os.makedirs(carpeta_files, exist_ok=True)
    ruta_archivo = os.path.join(carpeta_files, 'ANAC.xlsx')

    response = requests.get(url_archivo)
    with open(ruta_archivo, 'wb') as f:
        f.write(response.content)

    driver.quit()

    print(f"✅ Archivo descargado en: {ruta_archivo}")
    return ruta_archivo