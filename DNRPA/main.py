import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import os
import urllib3
import time

class ExtractDnrpa:

    def __init__(self):

        self.driver = None

    def extraer_tablas(self):

        wait = WebDriverWait(self.driver, 5)

        #Creacion de Driver y abrir pagina
        self.driver = webdriver.Chrome()
        self.driver.get('https://www.dnrpa.gov.ar/portal_dnrpa/estadisticas/rrss_tramites/tram_prov.php?origen=portal_dnrpa&tipo_consulta=inscripciones')

        
        #Buscamos el select de años
        elemento = self.driver.find_element(By.XPATH, '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[2]/td/select')
        
        # Obtener todas las opciones del elemento select - Unas de las opciones es un STR vacio
        opciones = elemento.find_elements(By.TAG_NAME, 'option')
        opciones.reverse() #--> Damos vuelta la lista


        #SELECTS de AUTOS Y MOTOS
        radio_de_auto = self.driver.find_element(By.XPATH, '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[4]/td/input[1]')
        radio_de_moto = self.driver.find_element(By.XPATH, '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[4]/td/input[2]')

        #Boton para abrir pag que contiene la tabla a buscar
        button_datos = self.driver.find_element(By.XPATH,'/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/center/input')


        #Recorremos las opciones 1 a 1 - Primero buscamos los datos de los autos
        for opcion in opciones:
            
            valor_opcion = opcion.get_attribute('value')

            if valor_opcion != '' and int(valor_opcion) >= 2014:
                
                #Elegimos el año
                opcion.click()

                #Eligimos la opcion 'autos' - Luego hacemos click en 'Aceptar'
                time.sleep(1)
                radio_de_auto.click()
                button_datos.click()

                time.sleep(3)


                tabla_auto = wait.until(EC.presence_of_element_located((By.XPATH,'/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/div/table')))



                time.sleep(1)
                radio_de_moto.click()
                button_datos.click()
                #Busqueda de datos del auto

            #Es para el valor vacio
            else: 
                pass


    



# Puedes agregar cualquier otra lógica después del bucle si es necesario
ExtractDnrpa().extraer_tablas()


#/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/div/table
#/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/div/table