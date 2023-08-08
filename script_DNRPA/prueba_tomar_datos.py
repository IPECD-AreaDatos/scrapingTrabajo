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

    #Objetivo: inicializar las variables a tratar.
    def __init__(self): 
         
        #Cargamos pagina
        self.driver = webdriver.Chrome()
        self.driver.get('https://www.dnrpa.gov.ar/portal_dnrpa/estadisticas/rrss_tramites/tram_prov.php?origen=portal_dnrpa&tipo_consulta=inscripciones')

        self.lista_año_cadena = []

    #Objetivo: Lo tomamos como Main. A partir de este se carga y se navega a traves de todas las paginas.
    def cargarPagina(self):             

        elemento = self.driver.find_element(By.XPATH, '//*[@id="seleccion"]/center/table/tbody/tr[2]/td/select')  #--> Año
        boton = self.driver.find_element(By.XPATH, '/html/body/div/section/article/div/div[2]/div/div/div/div/form/div/center/center/input') #--> Boton aceptar
        boton_auto = self.driver.find_element(By.XPATH, '//*[@id="seleccion"]/center/table/tbody/tr[4]/td/input[1]')#--> Tipo Vehiculo AUTO
        boton_moto = self.driver.find_element(By.XPATH,'/html/body/div/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[4]/td/input[2]') #--> Tipo Vehiculo MOTO

        botones = [boton_auto,boton_moto]

        # Obtenemos las opciones (años) y las damos vueltas para su uso.
        opciones = elemento.find_elements(By.TAG_NAME, 'option')
        opciones.reverse()
        self.lista_año_cadena = self.construir_lista_años()

        for tipo_v in botones:

            #Este iterador recorre las opciones del elemento <select>, elegimos los años del 2014 al año actual.
            for opcion in opciones:

                #Recuperamos ventana actual
        
                if opcion.get_attribute('value') in self.lista_año_cadena:

                    opcion.click()
                    tipo_v.click()
                    boton.click()

                    #Construimos los datos por provincia
                    self.datos_por_provincia(opcion.get_attribute('value'))

                    #Ventana actual
                    ventana_actual = self.driver.window_handles[-1]
                    self.driver.switch_to.window(ventana_actual)
                    self.driver.close()


                #Cambiar de vuelta a la ventana original
                ventana_original = self.driver.window_handles[0]
                self.driver.switch_to.window(ventana_original)



    #Objetivo: Construir una lista de los años a recuperar
    def construir_lista_años(self):

        año_actual = datetime.now().year
        
        #Rango de años a recorrer
        valor_deseado = range(2014,año_actual + 1)

        #Lista de años en formato de cadena - Lo usamos porque hay que navegar la web con datos en formato STR
        lista_año_cadena = []

        for i in valor_deseado: 

            lista_año_cadena.append(str(i)) #--> Agregamos los años en formato str (necesario para HTML)
    
        return lista_año_cadena


    def datos_por_provincia(self,año):

        driver = self.driver

        nueva_ventana = driver.window_handles[-1]  # El último handle corresponde a la nueva ventana

        # Cambiar al control de la nueva ventana
        driver.switch_to.window(nueva_ventana)

        #Buscamos la tabla de cada pagina. Luego rescatamos todos los links de cada registro.
        wait = WebDriverWait(driver, 1)
        archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div/section/article/div/div[2]/div/div/div/div/form/div/div/table")))
        etiquetas_a = archivo.find_elements(By.TAG_NAME, 'a')


        for a in etiquetas_a:

            driver.execute_script("window.open(arguments[0], '_blank');", a.get_attribute("href")) #--> Apertura del link en una nueva pestaña


            #PARA OBTENER LA TABLA DE CADA NUEVA PAG

            pestaña_localidad = driver.window_handles[-1]  # El último handle corresponde a la nueva ventana

            driver.switch_to.window(pestaña_localidad)

            wait = WebDriverWait(driver, 0)


            tabla = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/form/div/center/div/table")))

            self.obtener_valores(tabla,año)

            driver.close()

            # Cambiar de vuelta al control de la ventana original (pestaña anterior)
            pestaña_anterior = driver.window_handles[-1]
            driver.switch_to.window(pestaña_anterior)



    def obtener_valores(self,tabla_html,año):

        # Obtener todas las filas de la tabla
        filas = tabla_html.find_elements(By.TAG_NAME, 'tr')

        # Lista para almacenar los datos de la tabla
        tabla_datos = []
        
        for fila in filas:
            # Obtener las celdas de cada fila, excluyendo la última columna y la última celda de encabezado
            celdas = fila.find_elements(By.TAG_NAME, 'th') + fila.find_elements(By.TAG_NAME, 'td')[:-1]

            # Lista para almacenar los valores de cada fila
            fila_datos = []

            for celda in celdas:
                valor = celda.text
                if isinstance(valor, str):
                    # Verificar si el valor comienza con un número
                    if valor.strip() and valor[0].isdigit():
                        try:
                            # Reemplazar el punto decimal por una coma (si es necesario)
                            valor = valor.replace('.', '')
                            # Intentar convertir el valor a float
                            valor = int(valor)
                        except ValueError:
                            pass  # Mantener el valor original si no se puede convertir a float
                fila_datos.append(valor)
            # Verificar si la última celda es "Total" y eliminarla
            if fila_datos and fila_datos[-1] == "Total":
                fila_datos.pop()


            tabla_datos.append(fila_datos) 
        
        datos_sin_segunda_fila = tabla_datos[0:1] + tabla_datos[2:]
        # Transponer los datos utilizando pandas
        df = pd.DataFrame(datos_sin_segunda_fila)
        df_transpuesta = df.transpose()
        df_transpuesta = df_transpuesta.drop(df_transpuesta.index[-1])
        df_transpuesta = df_transpuesta.drop(df_transpuesta.columns[-1],axis=1)
        
        
        #Conversion de MESES a formato Y-M-D , tipo de dato: datetime
        meses = df_transpuesta[0][1:]

        #Donde almacenamos las nuevas fechas
        nuevas_fechas = list()

        for i in range(1, len(meses)+1):

            if i < 10:
                fecha_str =  '01-0'+str(i)+"-"+ str(año)
            else:
                fecha_str = '01-'+str(i)+"-"+ str(año)

            fecha_str = datetime.strptime(fecha_str,'%d-%m-%Y').date()
            nuevas_fechas.append(fecha_str)

        #Reasignacion de fechas
        df_transpuesta[0][1:] = nuevas_fechas


        print(df_transpuesta)

PruebaDnrpa().cargarPagina()