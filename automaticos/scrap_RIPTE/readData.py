import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
from selenium.webdriver.chrome.options import Options

class readData:

    def __init__(self):
        options = Options()
        options.add_argument('--headless')  # Ejecuta Chrome en modo sin cabeza
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
        self.driver = webdriver.Chrome(options=options)  # Reemplaza con la ubicación de tu ChromeDriver

        # URL de la página que deseas obtener
        self.url_pagina = 'https://datos.gob.ar/vi/dataset/sspm-remuneracion-imponible-promedio-trabajadores-estables-ripte'

    def descargar_archivo(self):
        # Cargar la página web
        self.driver.get(self.url_pagina)

        # Aumentar el tiempo de espera
        wait = WebDriverWait(self.driver, 10)
        
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        # Crear la carpeta "files" si no existe
        if not os.path.exists(carpeta_guardado):
            os.makedirs(carpeta_guardado)
        
        print("antes de fallar")
        print("TITULO DE LA PAGINA: ", self.driver.title)
        
        # Esperar hasta que el contenido específico esté disponible
        try:
            archivo_SP = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/div/div/div/div[1]/div[3]/div/div/a[2]")))
            print("DESPUES")

            # Obtener la URL del primer archivo
            url_archivo_SP = archivo_SP.get_attribute('href')

            # Nombre del primer archivo
            nombre_archivo_SP = 'ripte_historico.csv'

            # Descargar el primer archivo
            response_1 = requests.get(url_archivo_SP, verify=False)

            # Guardar el primer archivo en la carpeta especificada
            ruta_guardado_1 = os.path.join(carpeta_guardado, nombre_archivo_SP)
            with open(ruta_guardado_1, 'wb') as file:
                file.write(response_1.content)
        except:
            print("No se pudo encontrar el enlace al archivo dentro del tiempo de espera.")

    # Objetivo: extraer el último dato de ripte de la página de seguridad social. Luego el valor se reporta
    def extract_last_value(self):
        options = Options()
        options.add_argument('--headless')

        # Carga de página
        driver = webdriver.Chrome(options=options)
        driver.get('https://www.argentina.gob.ar/trabajo/seguridadsocial/ripte')

        # Buscamos la tabla que contiene los datos
        try:                                                   
            elemento = WebDriverWait(driver, 20).until(    
                EC.presence_of_element_located((By.XPATH, '/html/body/main/div[2]/div/section/article/div/div[9]/div/div/div/div/div[2]/div/div/span'))
            )
            contenido_texto = elemento.text
            contenido_numerico = contenido_texto.replace('$', '').replace('.', '').replace(',', '.')

            try:
                contenido_float = float(contenido_numerico)
            except ValueError:
                print("El contenido no es un número válido.")

            return contenido_numerico
        finally:
            driver.quit()
