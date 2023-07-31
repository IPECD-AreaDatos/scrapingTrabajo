import datetime
import mysql.connector
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import openpyxl
import pandas as pd
from datetime import datetime
import os



class PruebaDnrpa:

    def cargarPagina(self):

        #Cargamos pagina
        driver = webdriver.Chrome()
        driver.get('https://www.dnrpa.gov.ar/portal_dnrpa/estadisticas/rrss_tramites/tram_prov.php?origen=portal_dnrpa&tipo_consulta=inscripciones')


        # Obtener la ventana actual
        ventana_actual = driver.current_window_handle

        elemento = driver.find_element(By.XPATH, '//*[@id="seleccion"]/center/table/tbody/tr[2]/td/select')
        boton = driver.find_element(By.XPATH, '/html/body/div/section/article/div/div[2]/div/div/div/div/form/div/center/center/input')

        boton_opcion = driver.find_element(By.XPATH, '//*[@id="seleccion"]/center/table/tbody/tr[4]/td/input[1]')

        # Obtener todas las opciones del elemento <select>
        opciones = elemento.find_elements(By.TAG_NAME, 'option')

        opciones.reverse() #--> Damos vuelta la lista, porque los años estan invertidos

        
        lista_año_cadena = self.construir_lista_años()

        #Este iterador recorre las opciones del elemento <select>
        for opcion in opciones:
            
            if opcion.get_attribute('value') in lista_año_cadena:
                
                opcion.click() #--> Año
                boton_opcion.click() #--> Tipo Vehiculo
                boton.click() #--> Boton aceptar

        time.sleep(5)
  

        


    #Objetivo: Construir una lista de los años a recuperar
    def construir_lista_años(self):

         # Buscar la opción deseada por su valor y hacer clic en ella (2014 en adelante)
        año_actual = datetime.now().year
        
        #Rango de años a recorrer
        valor_deseado = range(2014,año_actual + 1) #--> Se suma 1 por el funcionamiento de range

        #Lista de años en formato de cadena - Lo usamos porque hay que navegar la web con datos en formato STR

        lista_año_cadena = []

        for i in valor_deseado: 

            lista_año_cadena.append(str(i))
    
        return lista_año_cadena




PruebaDnrpa().cargarPagina()