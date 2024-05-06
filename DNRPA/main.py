import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import time

class ExtractDnrpa:



    def extraer_tablas(self):
        
        #Creacion de Driver y abrir pagina
        driver = webdriver.Chrome()
        driver.get('https://www.dnrpa.gov.ar/portal_dnrpa/estadisticas/rrss_tramites/tram_prov.php?origen=portal_dnrpa&tipo_consulta=inscripciones')


        elemento = driver.find_element(By.XPATH, '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[2]/td/select')
        # Obtener todas las opciones del elemento select
        opciones = elemento.find_elements(By.TAG_NAME, 'option')

        for opcion in opciones:

            opcion.click()
            time.sleep(5)
            

# Puedes agregar cualquier otra lógica después del bucle si es necesario

        

ExtractDnrpa().extraer_tablas()