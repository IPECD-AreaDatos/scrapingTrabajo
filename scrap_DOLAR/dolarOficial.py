#Bibliotecas a utilizar
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import os
import pandas as pd
import urllib3
import time
from datetime import datetime
import mysql.connector
import numpy as np
import pandas as pd
import time
import os
import shutil

nuevos_datos = []

class dolarOficial:
        
    def descargaArchivo(self):
        # Obtener la ruta de la carpeta de guardado
        carpeta_guardado = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')

        # Limpiar la carpeta
        shutil.rmtree(carpeta_guardado, ignore_errors=True)
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

        #Dataframe correspondiente a los datos del dolar
        self.dataframe_dolar = pd.DataFrame(columns=['tipo_dolar','fecha','precio_compra','precio_cierre'])
    
        # Desactivar advertencias de solicitud no segura
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        # Cargar la página web
        self.driver.get(self.url_oficial)

        #Temporizador para que cargue la pagina
        wait = WebDriverWait(self.driver, 25)

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
        dia_anterior = str((fecha_actual.day)) +"/" + str(fecha_actual.month) + "/" + str(fecha_actual.year)

        #Fechas de inicio y fin
        fecha_desde.send_keys("01/01/2003")
        fecha_hasta.send_keys(dia_anterior)
        
        #Obtener boton
        boton = self.driver.find_element(By.ID,"DescargarID")

        boton.click()
        time.sleep(5)

        # Obtén el directorio de descarga
        directorio_descarga = carpeta_guardado

        # Espera un tiempo suficiente para que la descarga se complete
        time.sleep(10)
        
    def lecturaDolarOficial(self, host, user, database, password):
        
        conn = mysql.connector.connect(
            host='172.17.22.23', user='team-datos', password='HCj_BmbCtTuCv5}', database='ipecd_economico'
        )
        
        cursor= conn.cursor()
        
        
        table_name= 'dolar_oficial'
        directorio_actual= os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_name = "MyCsvLol.csv"
        file_path = os.path.join(ruta_carpeta_files, file_name)
        
        df = pd.read_csv(file_path, delimiter=';')
        df= df.replace({np.nan: None})

        df['Fecha cotizacion'] = pd.to_datetime(df['Fecha cotizacion'], format='%d/%m/%Y')
        df['Compra'] = df['Compra'].str.replace(',', '.').astype(float)
        df['Venta'] = df['Venta'].str.replace(',', '.').astype(float)
        df = df.drop(columns=['Unnamed: 3'], errors='ignore')
        print(df)
        
        longitud_datos_excel = len(df)
        print("Longitud Datos Dolar Oficial: ", longitud_datos_excel)
        
        select_row_count_query = "SELECT COUNT(*) FROM dolar_oficial"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base: ", filas_BD)
        
        if longitud_datos_excel != filas_BD:
            df_datos_nuevos = df.tail(longitud_datos_excel - filas_BD)
            
            print("Dolar Oficial")
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in range(len(df_datos_nuevos.columns))])})"
            for index, row in df_datos_nuevos.iterrows():
                data_tuple = tuple(row)
                conn.cursor().execute(insert_query, data_tuple)
                print(data_tuple)
                nuevos_datos.append(data_tuple)
            conn.commit()
            conn.close()
            print("Se agregaron nuevos datos")
        else:
            print("Se realizo una verificacion de la base de datos")