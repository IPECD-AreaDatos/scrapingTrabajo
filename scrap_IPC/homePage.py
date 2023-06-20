import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

class HomePage:
    
    # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
    #driver = webdriver.Chrome('C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scrap_IPC\\chromedriver.exe')  # Reemplaza con la ubicación de tu ChromeDriver
    driver_casa = webdriver.Chrome()

    # URL de la página que deseas obtener
    url_pagina = 'https://www.indec.gob.ar/indec/web/Nivel4-Tema-3-5-31'

    # Cargar la página web
    driver_casa.get(url_pagina)

    wait = WebDriverWait(driver_casa, 10)

    # Encontrar el enlace al archivo
    archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[2]/div[3]/div[2]/div[2]/div/div[2]/div/div[2]/div[1]/div[2]/div/div/a")))

    # Obtener la URL del archivo
    url_archivo = archivo.get_attribute('href')
    # Imprimir la URL del archivo
    print(url_archivo)
    
    # Ruta de la carpeta donde guardar el archivo
    #carpeta_guardado = 'C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scrap_IPC\\files\\xls'
    carpeta_guardado_casa = 'D:\\Users\\Pc-Pix211\\Desktop\\scrapingTrabajo\\scrap_IPC\\files\\xls'

    # Nombre del archivo
    nombre_archivo = 'archivo.xls'

    # Descargar el archivo
    response = requests.get(url_archivo)

    # Guardar el archivo en la carpeta especificada
    ruta_guardado = f'{carpeta_guardado_casa}\\{nombre_archivo}'
    with open(ruta_guardado, 'wb') as file:
        file.write(response.content)

    # Cerrar el navegador
    driver_casa.quit()
