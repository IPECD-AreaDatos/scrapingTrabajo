import os
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import urllib3
import time
import shutil
import zipfile


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
        # 1. Ir a la página principal
        driver.get('https://datos.anac.gob.ar/estadisticas/')
        time.sleep(2)

        # 2. Click en la categoría (div[12])
        print("🧭 Haciendo click en categoría...")
        categoria = wait.until(EC.element_to_be_clickable(
            (By.XPATH, '/html/body/main/section[2]/div/div[2]/div[12]/a/div/ul/li[2]/h3')
        ))
        categoria.click()

        # 3. Esperar redirección o nueva pestaña
        time.sleep(2)
        ventanas = driver.window_handles
        if len(ventanas) > 1:
            driver.switch_to.window(ventanas[-1])
            print("🪟 Cambiada a la nueva pestaña.")
        else:
            print("🔁 Redireccionado en la misma ventana.")

        # 3. Esperar que cargue el botón de descarga de ZIP
        print("🧭 Esperando botón 'Download all files'...")
        download_btn = wait.until(EC.presence_of_element_located(
            (By.XPATH, '/html/body/header/div[2]/span/a')
        ))

        url_zip = download_btn.get_attribute("href")
        
        # Definir ruta de descarga
        base_path = os.path.dirname(os.path.abspath(__file__))
        carpeta_files = os.path.abspath(os.path.join(base_path, '../../data/raw'))
        os.makedirs(carpeta_files, exist_ok=True)
        ruta_zip = os.path.join(carpeta_files, 'anac_data.zip')

        try:
            response = requests.get(url_zip, stream=True)
            response.raise_for_status()

            with open(ruta_zip, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"✅ ZIP descargado correctamente en: {ruta_zip}")

        except Exception as e:
            print(f"❌ Error al descargar el ZIP: {e}")
            return None
        
        try:
            nombre_excel = "series-historicas-2019-2025.xlsx"  # asegúrate que coincide con el nombre dentro del zip
            carpeta_destino = "/home/usuario/codigos/scrapingTrabajo/etl_modular/data/raw/"

            with zipfile.ZipFile(ruta_zip, 'r') as zip_ref:
                # Listar todos los archivos dentro del ZIP
                archivos_en_zip = zip_ref.namelist()
                
                # Buscar el archivo que queremos extraer
                archivo_objetivo = None
                for archivo in archivos_en_zip:
                    if nombre_excel in archivo:
                        archivo_objetivo = archivo
                        break
                
                if archivo_objetivo is None:
                    print(f"❌ No se encontró el archivo '{nombre_excel}' dentro del ZIP.")
                    return None
                
                # Extraer el archivo a la carpeta destino
                zip_ref.extract(archivo_objetivo, carpeta_destino)
                
                # Construir ruta completa del archivo extraído
                ruta_extraido = os.path.join(carpeta_destino, archivo_objetivo)
                print(f"✅ Archivo extraído en: {ruta_extraido}")
                return ruta_extraido
        
        except Exception as e:
            print(f"❌ Error al descomprimir el ZIP: {e}")
            return None

    except Exception as e:
        print(f"❌ Error durante la descarga: {e}")
        return None

    finally:
        driver.quit()