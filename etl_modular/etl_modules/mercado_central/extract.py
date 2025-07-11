import os
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib3
from pathlib import Path

import zipfile

def extract_mercado_central_data():
    print("📥 Iniciando descarga del archivo mercado central..")
    # === FRUTAS 2024 ZIP único ===
    ruta_frutas2024 = extraer_archivos(
        descripcion="Frutas 2024",
        xpath="/html/body/section/div/div/div[2]/div[3]/div/span[1]/a",
        carpeta_salida="frutas2024",
        nombre_zip="frutas2024"
    )

    # Descomprimir los ZIPs internos dentro de frutas2024
    descomprimir_todos_los_zips(
        carpeta_origen=ruta_frutas2024,
        carpeta_destino=ruta_frutas2024
    )

    # === FRUTAS 2025 múltiples ZIPs mensuales ===
    descargar_lote_zips(
        descripcion="Frutas 2025 (mensuales)",
        xpath_base="/html/body/section/div/div/div[2]/div[3]/div/span[position() > 1]/a",
        carpeta_destino="frutas2025"
    )

    # === HORTALIZAS 2024 ZIP único ===
    ruta_hortalizas2024 = extraer_archivos(
        descripcion="Hortalizas 2024",
        xpath="/html/body/section/div/div/div[2]/div[4]/div/span[1]/a",
        carpeta_salida="hortalizas2024",
        nombre_zip="hortalizas2024"
    )

    # Descomprimir los ZIPs internos dentro de hortalizas2024
    descomprimir_todos_los_zips(
        carpeta_origen=ruta_hortalizas2024,
        carpeta_destino=ruta_hortalizas2024
    )

    # === HORTALIZAS 2025 múltiples ZIPs mensuales ===
    descargar_lote_zips(
        descripcion="Hortalizas 2025 (mensuales)",
        xpath_base="/html/body/section/div/div/div[2]/div[4]/div/span[position() > 1]/a",
        carpeta_destino="hortalizas2025"
    )

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    return webdriver.Chrome(options=options)

def descargar_y_guardar_zip(url, ruta_destino):
    response = requests.get(url)
    with open(ruta_destino, 'wb') as f:
        f.write(response.content)
    print(f"✅ ZIP descargado en: {ruta_destino}")

def descomprimir_zip(ruta_zip, carpeta_destino):
    with zipfile.ZipFile(ruta_zip, 'r') as zip_ref:
        zip_ref.extractall(carpeta_destino)
    print(f"📂 Archivos extraídos en: {carpeta_destino}")

def descomprimir_todos_los_zips(carpeta_origen, carpeta_destino):
    carpeta_origen = Path(carpeta_origen)
    carpeta_destino = Path(carpeta_destino)
    for ruta_zip in carpeta_origen.iterdir():
        if ruta_zip.is_file() and ruta_zip.suffix == '.zip':
            carpeta_subdestino = carpeta_destino / ruta_zip.stem
            carpeta_subdestino.mkdir(parents=True, exist_ok=True)
            descomprimir_zip(ruta_zip, carpeta_subdestino)

def extraer_archivos(descripcion, xpath, carpeta_salida, nombre_zip):
    print(f"\n📥 Iniciando descarga de {descripcion}...")

    base_path = os.path.dirname(os.path.abspath(__file__))
    carpeta_files = os.path.abspath(os.path.join(base_path, f'../../data/raw/mercado_central/{carpeta_salida}'))
    os.makedirs(carpeta_files, exist_ok=True)

    driver = setup_driver()
    driver.get('https://mercadocentral.gob.ar/informaci%C3%B3n/precios-mayoristas')

    wait = WebDriverWait(driver, 10)
    elemento = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
    url_archivo = elemento.get_attribute('href')

    ruta_zip = os.path.join(carpeta_files, f'{nombre_zip}.zip')
    descargar_y_guardar_zip(url_archivo, ruta_zip)

    carpeta_extraer = os.path.join(carpeta_files, nombre_zip)
    os.makedirs(carpeta_extraer, exist_ok=True)
    descomprimir_zip(ruta_zip, carpeta_extraer)

    driver.quit()
    return carpeta_extraer

def descargar_lote_zips(descripcion, xpath_base, carpeta_destino):
    print(f"\n📥 Iniciando descarga de archivos por mes: {descripcion}")

    base_path = os.path.dirname(os.path.abspath(__file__))
    carpeta_files = os.path.abspath(os.path.join(base_path, f'../../data/raw/mercado_central/{carpeta_destino}'))
    os.makedirs(carpeta_files, exist_ok=True)

    driver = setup_driver()
    driver.get('https://mercadocentral.gob.ar/informaci%C3%B3n/precios-mayoristas')
    wait = WebDriverWait(driver, 10)

    # Selecciona todos los enlaces del bloque
    elementos = wait.until(EC.presence_of_all_elements_located((By.XPATH, xpath_base)))

    for elemento in elementos:
        nombre_archivo = elemento.text.strip().replace(" ", "_").replace("/", "-")
        url = elemento.get_attribute('href')
        ruta_zip = os.path.join(carpeta_files, f"{nombre_archivo}.zip")
        descargar_y_guardar_zip(url, ruta_zip)

    driver.quit()

    # Descomprimir todos los ZIPs descargados
    descomprimir_todos_los_zips(carpeta_files, carpeta_files)