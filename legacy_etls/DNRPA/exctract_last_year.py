from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import time
import pandas as pd
from datetime import datetime

class ExtractLastData:

    def __init__(self):
        self.driver = None
        self.original_window = None
        self.df_total = pd.DataFrame(columns=['fecha','id_provincia_indec','id_vehiculo','cantidad'])

     #Objetivo: extraer todas las tablas. La idea es usar esta funcion para una carga historica total.
    def extraer_tablas(self):
        """
        Extrae todas las tablas de la web de DNRPA y las carga en un Dataframe.
        
        Se utiliza el driver de Chrome en modo headless para no mostrar la ventana del navegador.
        Se abre la página de la web de DNRPA y se extraen los datos de AUTOS y MOTOS.
        Se transforman los numeros de la columna cantidad_vehiculos de str a int.
        Se imprime el tipo de dato de cada columna del DataFrame.
        Se devuelve el DataFrame con los datos extraidos.
        """
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
    
        self.driver = webdriver.Chrome(options=options)
        self.original_window = self.driver.current_window_handle         
        self.driver.get('https://www.dnrpa.gov.ar/portal_dnrpa/estadisticas/rrss_tramites/tram_prov.php?origen=portal_dnrpa&tipo_consulta=inscripciones')
        self.buscar_datos_de_tabla(1) #--> Datos de AUTOS
        self.buscar_datos_de_tabla(2) #--> Datos de MOTOS

        #Pasamos de STR a INT los numeros
        self.transformar_cantidad_vehiculos()
        return self.df_total


    #Objetivo: ver si estamos buscando un auto o moto y extraer esos datos
    def buscar_datos_de_tabla(self,tipo_vehiculo):

         # Buscamos el select de año
        elemento = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[2]/td/select'))
        )
        
        # Obtener todas las opciones del elemento select - Algunas opciones pueden ser cadenas vacías
        opciones = elemento.find_elements(By.TAG_NAME, 'option')
        opciones.reverse()  # Damos vuelta la lista - así obtenemos los años del 2014 al 20xx.
        anos_numeros = []

        for opcion in opciones:
            texto_opcion = opcion.text

            try:
                valor_numerico = int(texto_opcion)
                anos_numeros.append(valor_numerico)
            except ValueError:
                continue  

        #print("Años disponibles:", anos_numeros)

        ultimo_anio = max(anos_numeros)
        #print("Último año:", ultimo_anio)

        # Obtener los dos últimos años disponibles
        #ultimos_dos_anos = sorted(anos_numeros)[-2:]

        # Seleccionar el penúltimo año (el año anterior)
        #anteultimo_anio = ultimos_dos_anos[0]


        # Verificamos si se busca datos de moto o auto
        if tipo_vehiculo == 1:
            radio = self.driver.find_element(By.XPATH, '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[4]/td/input[1]')  # Autos
        elif tipo_vehiculo == 2:
            radio = self.driver.find_element(By.XPATH, '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[4]/td/input[2]')  # Motocicletas

        # Boton para abrir la página que contiene la tabla a buscar
        button_datos = self.driver.find_element(By.XPATH, '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/center/input')

        # Elegir solo el año más reciente (2024 en este caso)
        valor_opcion = str(ultimo_anio)
        try:
            opcion_seleccionada = elemento.find_element(By.XPATH, '//*[@id="seleccion"]/center/table/tbody/tr[2]/td/select/option[2]')
            opcion_seleccionada.click()
        except NoSuchElementException:
            print(f"No se encontró la opción con año {valor_opcion}.")
            return

        # Elegimos la opción 'autos' o 'motos' - Luego hacemos click en 'Aceptar'
        radio.click()
        button_datos.click()

        # Cambiar al último identificador de ventana (nueva pestaña)
        self.switch_to_latest_window()

        # Buscamos la tabla con los datos
        tabla = self.driver.find_element(By.XPATH, '//*[@id="seleccion"]/div/table')

        # Pasamos la tabla y la opcion, que sería el año
        self.construir_tabla(tabla, valor_opcion, tipo_vehiculo)

        # Cierra la pestaña actual y cambia nuevamente a la ventana original
        self.close_current_window()
        self.driver.switch_to.window(self.original_window)

    
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

        #print(datos)

        # Crea un DataFrame de pandas con los datos extraídos de la tabla
        df_formato_original = pd.DataFrame(datos)

        # ==== TRANSFORMACION DE LA TABLA   
        # 1 - Cambio de provincias por su ID correspondientes
        # 2 - Creamos fecha para asignar a las columnas
        # 3 - Transponemos las columnas para que el DF quede como FECHA | ID_PROV | ID_TIPO_VEHICULO | CANTIDAD

        # PASO 1 - Asignacion de ID's
        df_formato_original = df_formato_original.iloc[2:26, 0:13]
        #Transformacion de ciudades a su ID asignados por el INDEC
        df_formato_original[df_formato_original.columns[0]] = [6,2,10,14,18,22,26,30,34,38,42,46,50,54,58,62,66,70,74,78,82,86,90,94]

        # PASO 2 - CREACION Y ASIGNACION DE FECHAS
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

        if tipo_vehiculo == 1:
            nombre_vehiculo = 'AUTOS'
        else:
            nombre_vehiculo = 'MOTOS'

        #print(f"TABLA {nombre_vehiculo} DEL AÑO {valor_opcion}")

        self.df_total = pd.concat([self.df_total,df_melted])
    
        #print(self.df_total)
        
    def switch_to_latest_window(self):
        windows = self.driver.window_handles
        self.driver.switch_to.window(windows[-1])

    def close_current_window(self):
        self.driver.close()   

    def transformar_cantidad_vehiculos(self):

        self.df_total['cantidad'] = self.df_total['cantidad'].str.replace(".","")

        self.df_total['id_provincia_indec'] = self.df_total['id_provincia_indec'].astype(int)  # Convertir a tipo entero
    
        self.df_total['id_vehiculo'] = self.df_total['id_vehiculo'].astype(int)  # Convertir a tipo entero

        self.df_total['cantidad'] = self.df_total['cantidad'].astype(int)  # Convertir a tipo entero

        self.df_total['fecha'] = pd.to_datetime(self.df_total['fecha'], format='%Y-%m-%d')

        self.df_total = self.df_total[self.df_total['cantidad'] > 0]

        

