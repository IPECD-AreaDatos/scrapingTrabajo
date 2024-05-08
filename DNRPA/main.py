from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import time
import pandas as pd

class ExtractDnrpa:

    def __init__(self):

        self.driver = None
        self.original_window = None


    #Objetivo: extraer todas las tablas. La idea es usar esta funcion para una carga historica total.
    def extraer_tablas(self):

        # Creación de Driver y abrir página
        self.driver = webdriver.Chrome()
        self.original_window = self.driver.current_window_handle         
        self.driver.get('https://www.dnrpa.gov.ar/portal_dnrpa/estadisticas/rrss_tramites/tram_prov.php?origen=portal_dnrpa&tipo_consulta=inscripciones')
        self.buscar_datos_de_tabla(1) #--> Datos de AUTOS

        

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

                # Elegimos la opcion 'autos' - Luego hacemos click en 'Aceptar'
                time.sleep(1)
                radio.click()
                button_datos.click()
                time.sleep(3)

                # Cambiar al último identificador de ventana (nueva pestaña)
                self.switch_to_latest_window()

                #Buscamos la tabla con los datos
                tabla = self.driver.find_element(By.XPATH,'//*[@id="seleccion"]/div/table')

                #Pasamos la tabla y la opcion, que seria el año
                self.construir_tabla(tabla,valor_opcion)

                # Cierra la pestaña actual y cambia nuevamente a la ventana original
                self.close_current_window()
                self.driver.switch_to.window(self.original_window)

            # Es para el valor vacio
            else: 
                pass

    
    def construir_tabla(self,tabla,valor_opcion):

        # Extrae los datos de la tabla
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

        # Suponiendo que df es tu DataFrame actual
        df_formato_original = df_formato_original.iloc[2:26, 0:13]

        



    #Objetivo: tomar la ultima pestaña
    def switch_to_latest_window(self):
        # Cambia a la última ventana
        windows = self.driver.window_handles
        self.driver.switch_to.window(windows[-1])

    #Cerrar la pestaña actual
    def close_current_window(self):
        self.driver.close()   



# Puedes agregar cualquier otra lógica después del bucle si es necesario
#ExtractDnrpa().extraer_tablas()

import pandas as pd

# Tu DataFrame original
data = {
    0: ['BUENOS AIRES', 'C.AUTONOMA DE BS.AS', 'CATAMARCA', 'CORDOBA', 'CORRIENTES', 'CHACO', 'CHUBUT', 'ENTRE RIOS', 'FORMOSA', 'JUJUY', 'LA PAMPA', 'LA RIOJA', 'MENDOZA', 'MISIONES', 'NEUQUEN', 'RIO NEGRO', 'SALTA', 'SAN JUAN', 'SAN LUIS', 'SANTA CRUZ', 'SANTA FE', 'SGO.DEL ESTERO', 'TUCUMAN', 'T.DEL FUEGO'],
    1: [10.237, 6.052, 291, 3.771, 825, 665, 577, 750, 213, 371, 458, 155, 1474, 450, 1003, 532, 1073, 492, 333, 362, 2517, 404, 848, 312],
    2: [7.366, 4.680, 203, 2.645, 637, 448, 372, 684, 198, 312, 305, 98, 1015, 349, 791, 302, 584, 293, 234, 186, 2175, 300, 746, 290],
    3: [7.204, 4.857, 215, 2.498, 574, 457, 498, 663, 187, 336, 352, 129, 1140, 421, 824, 391, 805, 375, 243, 252, 2217, 299, 737, 301],
    4: [9.572, 6.410, 265, 3.355, 777, 589, 522, 764, 216, 384, 421, 139, 1328, 461, 1165, 458, 840, 492, 312, 356, 2654, 325, 982, 335],
    5: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    6: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    7: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    8: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    9: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    10: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    11: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
    12: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
}

# Crear DataFrame
df = pd.DataFrame(data)

# Renombrar las columnas
df.columns = ['CIUDAD', 'ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE']

# Convertir la tabla de valores a un formato largo (unstack)
df_melted = df.melt(id_vars=['CIUDAD'], var_name='MES', value_name='CANTIDAD')

# Imprimir DataFrame final
print(df_melted[df_melted['MES'] == 'FEBRERO'])




