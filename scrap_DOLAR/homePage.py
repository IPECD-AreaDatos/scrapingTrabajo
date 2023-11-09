"""
En este script lo que vamos a hacer es rescatar los valores de distintos tipos de dolares:

- Dolar oficial: de banco nacion
- Dolar MEP, BLUE y CCL: sacado de https://www.ambito.com/ --> Sitio web dedicado al ambito bursatil

"""

#Bibliotecas a utilizar
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import os
import pandas as pd

#Clase encargada de descargar
class HomePage:

    def __init__(self):

        # Configuración del navegador (en este ejemplo, se utiliza ChromeDriver)
        self.driver = webdriver.Chrome()  # Reemplaza con la ubicación de tu ChromeDriver

        # URL de las paginas del dolar
        self.url_oficial = 'https://www.bna.com.ar/Personas'
        self.url_blue = 'https://www.ambito.com/contenidos/dolar-informal-historico.html'
        self.url_mep = 'https://www.ambito.com/contenidos/dolar-mep.html'
        self.url_ccl = 'https://www.ambito.com/contenidos/dolar-cl.html'

        #Dataframe correspondiente a los datos del dolar
        self.dataframe_dolar = pd.DataFrame(columns=['tipo_dolar',''])

