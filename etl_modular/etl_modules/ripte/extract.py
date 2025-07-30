import os
import requests
import urllib3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def extract_ripte_data():
    print("📥 Iniciando descarga de RIPTE...")

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(options=options)
    driver.get("https://datos.gob.ar/vi/dataset/sspm-remuneracion-imponible-promedio-trabajadores-estables-ripte")

    wait = WebDriverWait(driver, 15)
    try:
        link = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/div/div/div/div[1]/div[3]/div/div/a[2]")))
        url_csv = link.get_attribute('href')

        carpeta_destino = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../data/raw'))
        os.makedirs(carpeta_destino, exist_ok=True)
        ruta_archivo = os.path.join(carpeta_destino, "ripte_historico.csv")

        response = requests.get(url_csv, verify=False)
        with open(ruta_archivo, "wb") as f:
            f.write(response.content)

        print(f"✅ Archivo RIPTE descargado en: {ruta_archivo}")
        return ruta_archivo

    finally:
        driver.quit()

def extract_latest_ripte_value():
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    print("📈 Extrayendo último valor de RIPTE desde el sitio web...")
    options = Options()
    options.add_argument('--headless')

    driver = webdriver.Chrome(options=options)
    driver.get('https://www.argentina.gob.ar/trabajo/seguridadsocial/ripte')

    try:
        elemento = WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/main/div[2]/div/section/article/div/div[9]/div/div/div/div/div[2]/div/div/span'))
        )
        texto = elemento.text.replace('$', '').replace('.', '').replace(',', '.')
        return float(texto)
    finally:
        driver.quit()
    