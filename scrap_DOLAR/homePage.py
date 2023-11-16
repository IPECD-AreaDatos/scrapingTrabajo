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
from selenium.webdriver.chrome.options import Options
import os
import pandas as pd
import urllib3
import time
from bs4 import BeautifulSoup
from datetime import datetime


#Clase encargada de descargar
class HomePage:

    def __init__(self):


        #Establecemos direccion para el archivo
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        #Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        chromeOptions = webdriver.ChromeOptions() #--> Instancia de crhome
        prefs = {"download.default_directory" : carpeta_guardado} #--> Directorio de descarga por defecto
        chromeOptions.add_experimental_option("prefs", prefs)
        self.driver = webdriver.Chrome(options=chromeOptions)

        # URLs de las paginas del dolar
        self.url_oficial = 'https://www.bna.com.ar/Personas'
        self.url_blue = 'https://www.ambito.com/contenidos/dolar-informal-historico.html'
        self.url_mep = 'https://www.ambito.com/contenidos/dolar-mep.html'
        self.url_ccl = 'https://www.ambito.com/contenidos/dolar-cl.html'

        #Dataframe correspondiente a los datos del dolar
        self.dataframe_dolar = pd.DataFrame(columns=['tipo_dolar','fecha','precio_compra','precio_cierre'])


    
    #Obtencion del archivo del dolar oficial de Banco Nacion
    def dolar_oficial(self):

        # Desactivar advertencias de solicitud no segura
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Cargar la página web
        self.driver.get(self.url_oficial)

        #Temporizador para que cargue la pagina
        wait = WebDriverWait(self.driver, 5)


        time.sleep(3)

        #Obtenemos el link que activa la funcion para abrir el tablero donde se ingresan las fechas para obtener los valores del dolar
        #archivo_id = wait.until(EC.presence_of_element_located((By.ID, "buttonHistoricoBilletes")))
        archivo_id = self.driver.find_element(By.ID,"buttonHistoricoBilletes")
        archivo_id.click() #--> Hacemos click sobre el elemento | Es necesario para poder acceder a los inputs


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

        
        boton.click()

        time.sleep(5)

    
    #Obtencion de datos del dolar blue de ambito
    def dolar_blue(self):

        # Desactivar advertencias de solicitud no segura
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Cargar la página web y esperamos un momento para que aparezcan las publicidades
        self.driver.get(self.url_blue)
        time.sleep(10)

        #Cerrar publicidad y POPUP de Notificaciones

        #boton_no_aceptar_notificacion = self.driver.find_element(By.XPATH,"/html/body/div[5]/div/div/div[2]/button[2]") #--> Boton de notificaciones
        #(self.driver, 10).until(EC.invisibility_of_element_located((By.XPATH,"/html/body/div[5]/div/div/div[2]/button[2]")))
        #WebDriverWait(self.driver, 10).until(EC.invisibility_of_element_located((By.XPATH,"/html/body/div[2]/div[2]/div/img")))
        #boton = self.driver.find_element(By.XPATH,"/html/body/div[5]/div/div/div[2]/button[2]")
        #boton.click()

        #cerrar_publicidad = self.driver.find_element(By.XPATH,"/html/body/div[2]/div[2]/div/img")#--> Imagen de X que se hace click para cerrar la publicidad
        #cerrar_publicidad.click()
        #cerrar_publicidad.click()

        #Obtencion de los inputs de fecha minima y maxima
        input_desde = self.driver.find_element(By.XPATH,"/html/body/main/div/div[1]/div[1]/div/div[2]/div[1]/div[1]/input")
        input_hasta = self.driver.find_element(By.XPATH,"/html/body/main/div/div[1]/div[1]/div/div[2]/div[1]/div[2]/input")



        #Fecha actual y la cadena del dia anterior
        fecha_actual = datetime.now()

        #Cadena del dia anterior
        dia_anterior = str((fecha_actual.day)- 1 ) +"/" + str(fecha_actual.month) + "/" + str(fecha_actual.year)

        #Fechas de inicio y fin
        input_desde.send_keys("01/01/2001")
        input_hasta.send_keys(dia_anterior)

        #Boton para ver datos
        self.driver.execute_script("document.querySelector('button.general-historical__button.boton').click()")


        time.sleep(5)
        



instancia = HomePage()
instancia.dolar_blue()



