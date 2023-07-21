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
            
            ruta_guardado = os.path.join(self.ruta_carpeta_files, nombre_archivo) #--> Ruta donde se guardara el archivo
            
            
                        
            #Ruta de guardado
            with open(ruta_guardado, 'wb') as archivo:
                archivo.write(response.content)
                
                
            #Añadimos a una lista la ruta del archivo para post-tratamiento
            self.lista_rutas.append(os.path.join(self.ruta_carpeta_files, nombre_archivo))
            
        
            
            
            



        
    #OBJETIVO: Construir DF a partir del DF descargado en 'obtener_estimacion(self)'
    def construir_df_estimaciones(self):
        
        
        #Columnas de cada DATAFRAME
        
        columnas = ['Departamento','<']
        
        #Iteramos por cada archivo - Cada provincia
        for i,j in enumerate(self.lista_rutas):
            
            
            #Rescate de valores
            df = pd.read_excel(j,skiprows=9,
                               usecols= 'A:Q',
                               nrows=35,
                               
                               )
            
            
            #Revisamos si hay valores nulos en la primera fila - Si hay borramos la primera fila 
            
            

            
            print(df.columns)
            
            
            # Revisar si hay valores nulos en la primera fila del DataFrame
            hay_nulos_en_primera_fila = df.iloc[0].isnull().any()

            # Imprimir el resultado
            print("Ruta del archivo:",j)
            print("Pronvicia que corresponde: " ,self.lista_provincias[i]  )
            print("¿Hay valores nulos en la primera fila del DataFrame?:", hay_nulos_en_primera_fila)
            
            print("=================================================")

            

    def imprimir_rutas(self):
            
        print(self.lista_rutas)         
            
        
            
            

        
