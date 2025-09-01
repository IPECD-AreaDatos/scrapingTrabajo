import os
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib3

def extract_supermercado_data():

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    #options = webdriver.ChromeOptions()
    #options.add_argument('--headless=new')  
    #options.add_argument('--disable-gpu')
    #options.add_argument('--no-sandbox')
    #options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome()
    driver.get('https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-1-34')

    wait = WebDriverWait(driver, 20)
    archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[2]/div[1]/div[2]/div[1]/div[2]/div/div/a")))

    url_archivo = archivo.get_attribute('href')

    # Ruta donde guardar el archivo
    base_path = os.path.dirname(os.path.abspath(__file__))
    carpeta_files = os.path.abspath(os.path.join(base_path, '../../data/raw'))
    os.makedirs(carpeta_files, exist_ok=True)
    ruta_archivo = os.path.join(carpeta_files, 'encuesta_supermercado.xls')

    response = requests.get(url_archivo)
    with open(ruta_archivo, 'wb') as f:
        f.write(response.content)

    driver.quit()

    return ruta_archivo
