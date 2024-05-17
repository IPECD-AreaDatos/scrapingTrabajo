from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import time
import pandas as pd
from datetime import datetime

class ExtractDnrpa:

    def __init__(self):

        self.driver = None
        self.original_window = None
        self.df_total = pd.DataFrame(columns=['id_provincia_indec','fecha','cantidad','id_vehiculo'])


    #Objetivo: extraer todas las tablas. La idea es usar esta funcion para una carga historica total.
    def extraer_tablas(self):

        # Creación de Driver y abrir página
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
    
        self.driver = webdriver.Chrome(options=options)
        self.original_window = self.driver.current_window_handle         
        self.driver.get('https://www.dnrpa.gov.ar/portal_dnrpa/estadisticas/rrss_tramites/tram_prov.php?origen=portal_dnrpa&tipo_consulta=inscripciones')
        self.buscar_datos_de_tabla(1) #--> Datos de AUTOS
        self.buscar_datos_de_tabla(2) #--> Datos de AUTOS

        return self.df_total


    #Objetivo: ver si estamos buscando un auto o moto y extraer esos datos
    def buscar_datos_de_tabla(self,tipo_vehiculo):

        #Buscamos el select de año
        elemento = self.driver.find_element(By.XPATH, '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[2]/td/select')
        
        # Obtener todas las opciones del elemento select - Unas de las opciones es un STR vacio
        opciones = elemento.find_elements(By.TAG_NAME, 'option')
        opciones.reverse() #--> Damos vuelta la lista - asi obtenemos los años del 2014 al 20xx. 

        #Verificamos si se busca datos de moto o auto
        if tipo_vehiculo == 1:
            radio = self.driver.find_element(By.XPATH, '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[4]/td/input[1]')
        elif tipo_vehiculo == 2:
            radio = self.driver.find_element(By.XPATH,'/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[4]/td/input[2]')

        #Boton para abrir pag que contiene la tabla a buscar
        button_datos = self.driver.find_element(By.XPATH,'/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/center/input')

        #Recorremos las opciones 1 a 1 - Primero buscamos los datos de los autos
        for opcion in opciones:
            
            valor_opcion = opcion.get_attribute('value')

            if valor_opcion != '' and int(valor_opcion) >= 2014:

                # Elegimos el año
                opcion.click()

                # Elegimos la opcion 'autos' o 'motos' - Luego hacemos click en 'Aceptar'
                #time.sleep(0.25)
                radio.click()
                button_datos.click()


                # Cambiar al último identificador de ventana (nueva pestaña)
                self.switch_to_latest_window()

                #Buscamos la tabla con los datos
                tabla = self.driver.find_element(By.XPATH,'//*[@id="seleccion"]/div/table')

                #Pasamos la tabla y la opcion, que seria el año
                self.construir_tabla(tabla,valor_opcion,tipo_vehiculo)

                # Cierra la pestaña actual y cambia nuevamente a la ventana original
                self.close_current_window()
                self.driver.switch_to.window(self.original_window)

            # Es para el valor vacio
            else: 
                pass

    
    def construir_tabla(self,tabla,valor_opcion,tipo_vehiculo):

        # ==== DATOS DE LA TABLA
        filas = tabla.find_elements(By.TAG_NAME, "tr")
        datos = []
        for fila in filas:
            celdas = fila.find_elements(By.TAG_NAME, "td")
            fila_datos = []
            for celda in celdas:
                fila_datos.append(celda.text)
            datos.append(fila_datos)

        # Crea un DataFrame de pandas con los datos extraídos de la tabla
        df_formato_original = pd.DataFrame(datos)

        # ==== TRANSFORMACION DE LA TABLA   
        # 1 - Cambio de provincias por su ID correspondientes
        # 2 - Creamos fecha para asignar a las columnas
        # 3 - Transponemos las columnas para que el DF quede como FECHA | ID_PROV | ID_TIPO_VEHICULO | CANTIDAD

        # PASO 1 - Asignacion de ID's

        # Suponiendo que df es tu DataFrame actual
        df_formato_original = df_formato_original.iloc[2:26, 0:13]

        #Transformacion de ciudades a su ID asignados por el INDEC
        df_formato_original[df_formato_original.columns[0]] = [6,2,10,14,18,22,26,30,34,38,42,46,50,54,58,62,66,70,74,78,82,86,90,94]


        # PASO 2 - CREACION Y ASIGNACION DE FECHAS

        # Lista para almacenar las fechas
        fechas = []

        # Iterar sobre los meses del año
        for mes in range(1, 13):
            # Crear la fecha
            fecha = datetime(int(valor_opcion), mes, 1) #--> valor_opcion es el año en cuestion

            # Formatear la fecha como "año-mes-01"
            fecha_formateada = fecha.strftime("%Y-%m-%d")
            # Agregar la fecha formateada a la lista
            fechas.append(fecha_formateada)

    
        #Vamos a crear una lista que sea las nuevas columnas, y esta debe ser [ID_PROV,MES1,MES2,...,MES12]
        nuevos_nombres_columnas = ['id_provincia_indec'] + fechas
        
        #Asignacion de columnas
        df_formato_original.columns = nuevos_nombres_columnas


        # PASO 3: trasponemos el dataframe - Las cantidad las "acostamos"
        df_melted = df_formato_original.melt(id_vars=['id_provincia_indec'], var_name='fecha', value_name='cantidad')

        df_melted['id_vehiculo'] = tipo_vehiculo

        print(df_melted)

        self.df_total = pd.concat([self.df_total,df_melted])

    #Objetivo: tomar la ultima pestaña
    def switch_to_latest_window(self):
        # Cambia a la última ventana
        windows = self.driver.window_handles
        self.driver.switch_to.window(windows[-1])

    #Cerrar la pestaña actual
    def close_current_window(self):
        self.driver.close()   


