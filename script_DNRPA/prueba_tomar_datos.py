import datetime
import mysql.connector
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import openpyxl
import pandas as pd
from datetime import datetime
import os
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup



class PruebaDnrpa:

    def __init__(self): 
         
        #Cargamos pagina
        self.driver = webdriver.Chrome()
        self.driver.get('https://www.dnrpa.gov.ar/portal_dnrpa/estadisticas/rrss_tramites/tram_prov.php?origen=portal_dnrpa&tipo_consulta=inscripciones')

    def cargarPagina(self):             

        elemento = self.driver.find_element(By.XPATH, '//*[@id="seleccion"]/center/table/tbody/tr[2]/td/select')
        boton = self.driver.find_element(By.XPATH, '/html/body/div/section/article/div/div[2]/div/div/div/div/form/div/center/center/input')
        boton_opcion = self.driver.find_element(By.XPATH, '//*[@id="seleccion"]/center/table/tbody/tr[4]/td/input[1]')

        # Obtener todas las opciones del elemento <select>
        opciones = elemento.find_elements(By.TAG_NAME, 'option')

        opciones.reverse() #--> Damos vuelta la lista, porque los años estan invertidos

        lista_año_cadena = self.construir_lista_años()

        #Lista de todos los links a recorrer con BS
        links = []
       
        #Este iterador recorre las opciones del elemento <select>
        for opcion in opciones:
       
            if opcion.get_attribute('value') in lista_año_cadena:

                opcion.click() #--> Año
                boton_opcion.click() #--> Tipo Vehiculo
                boton.click() #--> Boton aceptar

                #Obtenemos todos los links
                links.extend(self.obtenerlinks_por_provincia())

            
            # Cambiar de vuelta a la ventana original
            ventana_original = self.driver.window_handles[0]
            self.driver.switch_to.window(ventana_original)


            
        print(links)


    #Objetivo: Construir una lista de los años a recuperar
    def construir_lista_años(self):

        año_actual = datetime.now().year
        
        #Rango de años a recorrer
        valor_deseado = range(2014,año_actual + 1)

        #Lista de años en formato de cadena - Lo usamos porque hay que navegar la web con datos en formato STR

        lista_año_cadena = []

        for i in valor_deseado: 

            lista_año_cadena.append(str(i))
    
        return lista_año_cadena


    def obtenerlinks_por_provincia(self):

        driver = self.driver

        nueva_ventana = driver.window_handles[-1]  # El último handle corresponde a la nueva ventana

        # Cambiar al control de la nueva ventana
        driver.switch_to.window(nueva_ventana)


        wait = WebDriverWait(driver, 1)
        archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div/section/article/div/div[2]/div/div/div/div/form/div/div/table")))


        etiquetas_a = archivo.find_elements(By.TAG_NAME, 'a')


        for a in etiquetas_a:

            driver.execute_script("window.open(arguments[0], '_blank');", a.get_attribute("href"))

            nueva_ventana = driver.window_handles[-1]  # El último handle corresponde a la nueva ventana

            


        #Tomamos el div por un HTML aparte
        html_div = archivo.get_attribute('outerHTML')
                                                                        

        #Buscamos todos las etiquetas <a> , ventaja: todos tienen la misma clase: 'a-color2'

        soup = BeautifulSoup(html_div,'html.parser')

        # Encuentra todas las etiquetas <a> dentro del div
        etiquetas_a = soup.find_all('a')

        #Obtenemos todos los links
        hrefs = [a['href'] for a in etiquetas_a]

        #Agregamos link inicial
        lista_hrefs = [] 

        for i in hrefs:

            lista_hrefs.append('https://www.dnrpa.gov.ar/portal_dnrpa/estadisticas/rrss_tramites/' + i )


        return lista_hrefs

PruebaDnrpa().cargarPagina()