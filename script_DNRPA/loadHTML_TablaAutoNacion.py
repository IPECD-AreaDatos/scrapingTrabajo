import datetime
from bs4 import BeautifulSoup
import mysql.connector
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.select import Select
import time


host = 'localhost'
user = 'root'
password = 'Estadistica123'
database = 'prueba1'

class loadHTML_TablaAutoNacion:
    def loadInDataBase(self, host, user, password, database):
        # Se toma el tiempo de comienzo
        start_time = time.time()

        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        # Crear el cursor para ejecutar consultas
        cursor = conn.cursor()
        try:
            driver = webdriver.Chrome()
            driver.get('https://www.dnrpa.gov.ar/portal_dnrpa/estadisticas/rrss_tramites/tram_prov.php?origen=portal_dnrpa&tipo_consulta=inscripciones')
            time.sleep(5)
            elemento = driver.find_element(By.XPATH, '//*[@id="seleccion"]/center/table/tbody/tr[2]/td/select')
            # Obtener todas las opciones del elemento select
            opciones = elemento.find_elements(By.TAG_NAME, 'option')

            # Buscar la opción deseada por su valor y hacer clic en ella
            valor_deseado = '2020'  # Valor de la opción que deseas seleccionar

            for opcion in opciones:
                if opcion.get_attribute('value') == valor_deseado:
                    opcion.click()
                    break
            
            # Se toma el tiempo de finalización y se c  alcula
            end_time = time.time()
            duration = end_time - start_time
            print("-----------------------------------------------")
            print("Se guardaron los datos de SIPA NACIONAL CON ESTACIONALIDAD")
            print("Tiempo de ejecución:", duration)

            # Cerrar la conexión a la base de datos
            conn.close()

        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")
            conn.close()  # Cerrar la conexión en caso de error
            
loadHTML_TablaAutoNacion().loadInDataBase(host, user, password, database)