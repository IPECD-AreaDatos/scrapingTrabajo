"""
En este script lo que vamos a hacer es rescatar los valores de distintos tipos de dolares:

- Dolar oficial: de banco nacion
- Dolar MEP, BLUE y CCL: sacado de https://www.ambito.com/ --> Sitio web dedicado al ambito bursatil

"""

#Bibliotecas a utilizar
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import os
import pandas as pd
import urllib3
import time
from bs4 import BeautifulSoup
from datetime import datetime


#Clase encargada de descargar
class HomePage:

    def __init__(self):

        # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
        self.driver = webdriver.Chrome()

        # URLs de las paginas del dolar
        self.url_oficial = 'https://www.bna.com.ar/Personas'
        self.url_blue = 'https://www.ambito.com/contenidos/dolar-informal-historico.html'
        self.url_mep = 'https://www.ambito.com/contenidos/dolar-mep.html'
        self.url_ccl = 'https://www.ambito.com/contenidos/dolar-cl.html'

        #Dataframe correspondiente a los datos del dolar
        self.dataframe_dolar = pd.DataFrame(columns=['tipo_dolar','fecha','precio_compra','precio_cierre'])


    

    def dolar_oficial(self):

        # Desactivar advertencias de solicitud no segura
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


        # Obtiene el código HTML de la página
        html = self.driver.page_source

        # Crea una instancia de BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Cargar la página web
        self.driver.get(self.url_oficial)

        #Temporizador para que cargue la pagina
        wait = WebDriverWait(self.driver, 5)


        time.sleep(3)

        #Obtenemos el link que activa la funcion para abrir el tablero donde se ingresan las fechas para obtener los valores del dolar
        #archivo_id = wait.until(EC.presence_of_element_located((By.ID, "buttonHistoricoBilletes")))
        archivo_id = self.driver.find_element(By.ID,"buttonHistoricoBilletes")
        archivo_id.click() #--> Hacemos click sobre el elemento | Es necesario para poder acceder a los inputs
    
        #modificacion del html
        input_desde = soup.find()

        #Esperamos que sean clickeables y los almacenamos en variables
        wait.until(EC.element_to_be_clickable((By.ID, "fechaDesde")))
        wait.until(EC.element_to_be_clickable((By.ID, "fechaHasta")))


        fecha_desde = self.driver.find_element(By.ID,"fechaDesde")
        fecha_hasta = self.driver.find_element(By.ID,"fechaHasta")


        
        # Elimina el atributo "readonly" del elemento
        self.driver.execute_script("document.getElementById('fechaDesde').removeAttribute('readonly')")
        self.driver.execute_script("document.getElementById('fechaHasta').removeAttribute('readonly')")


        #Fecha actual y la cadena del dia anterior
        fecha_actual = datetime.now()

        #Cadena del dia anterior
        dia_anterior = str((fecha_actual.day)- 1 ) +"/" + str(fecha_actual.month) + "/" + str(fecha_actual.year)

        #Fechas de inicio y fin
        fecha_desde.send_keys("01/01/2003")
        fecha_hasta.send_keys(dia_anterior)
        

        #Obtener boton
        boton = self.driver.find_element(By.ID,"DescargarID")

        #Establecemos direccion para el archivo
        

        boton.click()

        time.sleep(5)


instancia = HomePage()
instancia.dolar_oficial()



