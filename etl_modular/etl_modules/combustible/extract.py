from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import requests

def extract_combustible_data():
    print("📥 Iniciando descarga del archivo COMBUSTIBLE desde la web...")
    
    # Configurar las opciones de Chrome para ejecución en segundo plano
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')  # Modo sin cabeza (sin interfaz gráfica)
    options.add_argument('--disable-gpu')  # Deshabilitar GPU, útil para headless en algunos sistemas
    
    # Inicializar el navegador
    driver = webdriver.Chrome(options=options)

    # URL de la página que contiene los datos
    url_pagina = 'https://datos.gob.ar/dataset/energia-refinacion-comercializacion-petroleo-gas-derivados-tablas-dinamicas/archivo/energia_f0e4e10a-e4b8-44e6-bd16-763a43742107'

    try:
        # Cargar la página web
        driver.get(url_pagina)
            
        # Esperar hasta que el enlace del archivo esté disponible
        wait = WebDriverWait(driver, 10)
        archivo_element = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/div/div/div[3]/a[1]")))
            
        # Obtener la URL del archivo
        url_archivo = archivo_element.get_attribute('href')
            
        # Configurar el directorio de descarga
        base_path = os.path.dirname(os.path.abspath(__file__))
        carpeta_files = os.path.join(base_path, '../../data/raw')

        os.makedirs(carpeta_files, exist_ok=True)

        ruta_archivo = os.path.join(carpeta_files, 'venta_combustible.csv')

        # Descargar el archivo
        response = requests.get(url_archivo)
            
        # Guardar el archivo en el directorio especificado
        with open(ruta_archivo, 'wb') as f:
            f.write(response.content)

    except Exception as e:
        print(f"Error al descargar el archivo: {str(e)}")
        
    finally:
        # Cerrar el navegador después de completar la operación
        driver.quit()

    print(f"✅ Archivo descargado en: {ruta_archivo}")
    return ruta_archivo