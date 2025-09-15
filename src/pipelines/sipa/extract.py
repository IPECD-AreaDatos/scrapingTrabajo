import os
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib3

def extract_sipa_data():

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(options=options)
    driver.get('https://www.argentina.gob.ar/trabajo/estadisticas')

    wait = WebDriverWait(driver, 10)
    archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/main/div[2]/div/section[2]/div/div[9]/div/div/table/tbody/tr[1]/td[4]/a")))
    url_archivo = archivo.get_attribute('href')

    # Ruta donde guardar el archivo (siempre en data/raw/ fuera de src)
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
    carpeta_files = os.path.join(project_root, 'data', 'raw')
    os.makedirs(carpeta_files, exist_ok=True)
    ruta_archivo = os.path.join(carpeta_files, 'encuesta_supermercado.xls')

    response = requests.get(url_archivo)
    with open(ruta_archivo, 'wb') as f:
        f.write(response.content)

    driver.quit()

    return ruta_archivo
