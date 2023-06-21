import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

class HomePage:
    
    # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
    driver = webdriver.Chrome()  # Reemplaza con la ubicación de tu ChromeDriver

    # URL de la página que deseas obtener
    url_pagina = 'https://www.trabajo.gob.ar/estadisticas/index.asp'

    # Cargar la página web
    driver.get(url_pagina)

    wait = WebDriverWait(driver, 10)

    # Encontrar el enlace al archivo
    archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/main/div[2]/div/section[2]/article/div/div[2]/div[2]/a")))

    # Obtener la URL del archivo
    url_archivo = archivo.get_attribute('href')
    # Imprimir la URL del archivo
    print(url_archivo)
    
    # Ruta de la carpeta donde guardar el archivo
    carpeta_guardado = 'C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scrap_SIPA\\files'
    #carpeta_guardado_casa = 'D:\\Users\\Pc-Pix211\\Desktop\\scrapingTrabajo\\scrap_SIPA\\files'

    # Nombre del archivo
    nombre_archivo = 'SIPA.xlsx'

    # Descargar el archivo
    response = requests.get(url_archivo)

    # Guardar el archivo en la carpeta especificada
    ruta_guardado = f'{carpeta_guardado}\\{nombre_archivo}'
    with open(ruta_guardado, 'wb') as file:
        file.write(response.content)

    # Cerrar el navegador
    driver.quit()
