import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import os
import urllib3
import pandas as pd
import xlrd
from bs4 import BeautifulSoup
import datetime


class homePage:
    
    #Declaracion e inicializacion de variables de clase
    def __init__(self): 
        
        
        self.driver = webdriver.Chrome() #Navegador a utilizar

        self.url_estimaciones = 'https://www.indec.gob.ar/indec/web/Nivel4-Tema-2-24-119' #URL de todas las estimaciones, del 2010 al 2025.
        
        
        self.df_estimaciones = pd.DataFrame()
        
        
        try:
            
            # Obtener la ruta del directorio actual (donde se encuentra el script)
            self.directorio_actual = os.path.dirname(os.path.abspath(__file__))
            
            #Ruta donde se alacenaran los XLS
            self.ruta_carpeta_files = os.path.join(self.directorio_actual, 'files')      
            
            
            self.lista_provincias = list()
            self.lista_rutas = list()
                 
        except Exception as e:
            
            pass
            

    #OBJETIVO: Descargar archivo de estimaciones del periodo 2010-2040.
    def descargar_archivos(self): 
        
        #Cargamos pagina
        self.driver.get(self.url_estimaciones)

    
        # Desactivar advertencias de solicitud no segura
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        #Obtenemos DIV que contiene los <a> de interes
        
        wait = WebDriverWait(self.driver, 10)
        archivo = wait.until(EC.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[2]/div[3]/div/div[2]/div/div[2]/div/div[2]")))
        
        
        #Tomamos el div por un HTML aparte
        html_div = archivo.get_attribute('outerHTML')
        
        
        #Buscamos todos las etiquetas <a> , ventaja: todos tienen la misma clase: 'a-color2'
        
        soup = BeautifulSoup(html_div,'html.parser')
        
        # Encuentra todas las etiquetas <a> dentro del div
        etiquetas_a = soup.find_all('a',class_='a-color2')
        
        #Obtenemos todos los links
        hrefs = [a['href'] for a in etiquetas_a]
        
                
        #Nombre de cada archivo
        self.lista_provincias = [a.text for a in etiquetas_a]
        
        
        for i,j in enumerate(self.lista_provincias):
            
            self.lista_provincias[i] = self.lista_provincias[i].replace(" ","")

        for indice,href in enumerate(hrefs):
                    
            #Construccion del link, se concatenera con el link del indec, ya que los <a> no lo contienen directamente    
            link = 'https://www.indec.gob.ar/'+str(href)
        
        
            #Descargar archivo
            response = requests.get(link)
        
        
            #Nombre - Asginar el texto que se encuentra en los links
            nombre_archivo = str(self.lista_provincias[indice])+".xls" #--> Extesion de archivo

           # print(f"""          ====================================================================================================
           # Se descargo el archivo de {nombre_archivo} con exito
         #==================================================================================================== """)
            
            ruta_guardado = os.path.join(self.ruta_carpeta_files, nombre_archivo) #--> Ruta donde se guardara el archivo
            
            
                        
            #Ruta de guardado
            with open(ruta_guardado, 'wb') as archivo:
                archivo.write(response.content)
                
                
            #Añadimos a una lista la ruta del archivo para post-tratamiento
            self.lista_rutas.append(os.path.join(self.ruta_carpeta_files, nombre_archivo))
            
#OBJETIVO: Construir DF a partir del DF descargado en 'obtener_estimacion(self)'
    def construir_df_estimaciones(self):
        
        
        """
        Valores para moverse en los excels
        """
        filas_iniciales = [9,10,9,9,9,9,9,9,9,9,9,10,9,9,9,10,10,10,10,10,10,10,10,10]
        filas_finales = [24,34,25,34,24,35,34,26,18,25,31,28,27,26,25,23,33,29,19,17,29,37,13,27]
        id_provincia = [2, 6, 10, 22, 23, 14, 18, 30, 34, 38, 42, 46, 50, 54, 58, 62, 66, 70, 74, 78, 82, 86, 94, 90]
        
        
        inicio = 0 #--> indice que iterara en los indices de valores iniciales
        final = 0 #--> indice que iterara en los indices de valores finales
        codigo = 0
        

        # Lista para almacenar los valores de las filas
        target_rows_values = []

        lista_valores = [] #--> Contendra la poblacion estimada de cada año
        lista_años = [] #--> Contendra de forma ordenada los años (del 2010 al 2025)
        lista_deps = [] #--> Contendra los departamentos de cada provincia (POR EL MOMENTO FORMATO DE STR)

        rango_años = [2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020,2021,2022,2023,2024,2025]
        fecha_row_index = 4

        """
                        
                                   
        """
        lista_rutas = self.lista_rutas
    

        for i in lista_rutas:

            print(inicio)
            print(final)

            workbook = xlrd.open_workbook(i) #--> abrimos doc
            sheet = workbook.sheet_by_index(0) #--> TOmamos primera hoja

            anios = [int(anio) for anio in sheet.row_values(fecha_row_index)[1:] if isinstance(anio, (int, float))]
            valores_por_anio_y_localidad = []
            
            #Carga de CABA

            for row_index in range(filas_iniciales[inicio], min(sheet.nrows, filas_finales[final])):
                localidad = sheet.cell_value(row_index, 0).strip()
                valores_por_localidad = sheet.row_values(row_index)[1:]
                for i, valor in enumerate(valores_por_localidad):
                    if isinstance(valor, (int, float)):
                        anio = anios[i]
                        fecha = datetime.date(year=anio, month=1, day=1)
                        valores_por_anio_y_localidad.append({
                            "Anio": fecha,
                            "Provincia": id_provincia[codigo],
                            "Localidad": localidad,
                            "Valor": valor
                        })
            

            inicio = inicio + 1
            final = final + 1 
            codigo = codigo + 1
            
            
        df = pd.DataFrame()
        df['valores'] = lista_valores
        df['años'] = lista_años
        df['depar'] = lista_deps


        for i in df.values:

            print(i)

    def imprimir_rutas(self):
            
       print(self.lista_rutas)


instancia = homePage()

instancia.descargar_archivos()
instancia.construir_df_estimaciones()