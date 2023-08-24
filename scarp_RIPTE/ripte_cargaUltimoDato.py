import time
import mysql.connector
from selenium import webdriver
from selenium.webdriver.common.by import By
import re

class ripte_cargaUltimoDato:
    def loadInDataBase(self, host, user, password, database):  
        # Se toma el tiempo de comienzo
        start_time = time.time()

        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        
        driver = webdriver.Chrome()
        driver.get('https://www.argentina.gob.ar/trabajo/seguridadsocial/ripte')
       
        elemento = driver.find_element(By.XPATH, '//*[@id="block-system-main"]/section/article/div/div[9]/div/div/div/div/div[1]/div/h3')
        contenido_texto = elemento.text
        contenido_numerico = contenido_texto.replace('$', '').replace('.','').replace(',', '.')

        try:
            contenido_float = float(contenido_numerico)
            print("Contenido como float:", contenido_float)
        except ValueError:
            print("El contenido no es un número válido.")


https://www.argentina.gob.ar/trabajo/seguridadsocial/ripte

https://datos.gob.ar/dataset/sspm-salario-minimo-vital-movil-pesos-corrientes/archivo/sspm_57.1