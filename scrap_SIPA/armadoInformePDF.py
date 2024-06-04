import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import os

class googleSheets:
    def pruebaAbrirCarpeta(self):
        #self.descargaArchivo()

        # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
        driver = webdriver.Chrome()  # Reemplaza con la ubicación de tu ChromeDriver

        # URL de la página que deseas obtener
        url_pagina = 'https://drive.google.com/drive/folders/1PoOm5b2lzpqG0JH94eqnFhyNfl9bhSNU?usp=sharing'

        # Cargar la página web
        driver.get(url_pagina)


        # Esperar hasta que el elemento esté presente en la página
        hojaCalculoSIPA = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, '//div[@aria-label="Hojas de cálculo de Google: Automatizar Sipa"]'))
        )

        # Ahora puedes interactuar con el elemento según tus necesidades
        print("Elemento encontrado:", hojaCalculoSIPA.text)

        # Hacer clic en el elemento
        hojaCalculoSIPA.click()


        # Esperar hasta que el elemento esté presente en la página
        elemento = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, '//div[@style="width: 173px; height: 48px; background-size: 173px 48px; background-image: url(\'https://docs.google.com/drawings/u/0/d/s6uIHrtC0E8SH2pFnTZEvOQ/image?w=173&h=48&rev=17&drawingRevisionAccessToken=admat8PnhakYyQ&ac=1&fmt=svg&parent=1GuqZN2aWTo31C6-bdF1FrXCzU4snGKKwa877FZaoXNE\'); background-position: left top; background-repeat: no-repeat;"]'))
        )

        # Hacer clic en el elemento
        elemento.click()

        # Esperar hasta que la página cargue (puedes ajustar el tiempo según tus necesidades)
        espera = WebDriverWait(driver, 60)
        espera.until(EC.invisibility_of_element_located((By.XPATH, '//div[@aria-label="Hojas de cálculo de Google: Automatizar Sipa"]')))
        # Cerrar el navegador al finalizar
        driver.quit()

    def descargaArchivo(self):
        # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
        driver = webdriver.Chrome()  # Reemplaza con la ubicación de tu ChromeDriver

        # URL de la página que deseas obtener
        url_pagina = 'https://www.argentina.gob.ar/trabajo/estadisticas/situacion-y-evolucion-del-trabajo-registrado'

        # Cargar la página web
        driver.get(url_pagina)

        wait = WebDriverWait(driver, 10)

        # Encontrar el enlace al archivo
        archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/main/div[2]/div/section/article/div/div[9]/div/div/div/div/p[3]/a[1]")))

        # Obtener la URL del archivo
        url_archivo = archivo.get_attribute('href')
        # Imprimir la URL del archivo
        print(url_archivo)
        
        # Ruta de la carpeta donde guardar el archivo
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        # Nombre del archivo
        nombre_archivo = 'DocumentoDeCargaExcel.xlsx'

        # Descargar el archivo
        response = requests.get(url_archivo)

        # Guardar el archivo en la carpeta especificada
        ruta_guardado = f'{carpeta_guardado}\\{nombre_archivo}'
        with open(ruta_guardado, 'wb') as file:
            file.write(response.content)

        # Cerrar el navegador
        driver.quit()

