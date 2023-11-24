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

class dolarBlue:
    def tomaDolarBlue(self):
        self.url_blue = 'https://www.ambito.com/contenidos/dolar-informal-historico.html'
        self.driver = webdriver.Chrome()  # Reemplaza con la ubicación de tu ChromeDriver
        
        
        # Desactivar advertencias de solicitud no segura
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.driver.get(self.url_blue)
        time.sleep(10)
        
        wait = WebDriverWait(self.driver, 10)
        # Verificar si hay iframes presentes
        iframes = self.driver.find_elements(By.TAG_NAME, "iframe")
        
        popup = wait.until(EC.element_to_be_clickable((By.ID, "onesignal-slidedown-cancel-button")))
        self.driver.execute_script("arguments[0].scrollIntoView();", popup)
        popup.click()
        
        # Esperar a que el iframe desaparezca
        wait.until(EC.invisibility_of_element_located((By.ID, "google_ads_iframe_/78858960/Ambito/Not-HE_0")))

        wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "general-historical__datepicker.datepicker.desde.form-control")))
        wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "general-historical__datepicker.datepicker.hasta.form-control")))

        # Encontrar elementos de fecha desde y fecha hasta
        fecha_desde = self.driver.find_element(By.CLASS_NAME, "general-historical__datepicker.datepicker.desde.form-control")
        fecha_hasta = self.driver.find_element(By.CLASS_NAME, "general-historical__datepicker.datepicker.hasta.form-control")

        # Fecha actual y cadena del día anterior
        fecha_actual = datetime.now()

        # Cadena del día anterior
        dia_anterior = str((fecha_actual.day)) + "/" + str(fecha_actual.month) + "/" + str(fecha_actual.year)

        # Fechas de inicio y fin
        fecha_desde.clear()
        fecha_desde.send_keys("01/01/2003")
        fecha_hasta.clear()
        fecha_hasta.send_keys(dia_anterior)
        
        # Esperar hasta que el botón sea clickeable
        boton = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "general-historical__button.boton")))
        # Desplazarse hasta el elemento
        ActionChains(self.driver).move_to_element(boton).perform()
        boton.click()
        time.sleep(20)
        table = self.driver.find_element(By.CLASS_NAME, 'general-historical__table')

        # Obtener el HTML de la tabla
        table_html = table.get_attribute('outerHTML')

        # Reemplazar comas por puntos en los datos
        table_html = table_html.replace(',', '.')

        # Leer la tabla HTML en un DataFrame de pandas
        df = pd.read_html(table_html)[0]
        print(df)
        
        
        
        
        
        self.url_mep = 'https://www.ambito.com/contenidos/dolar-mep-historico.html'
        self.url_ccl = 'https://www.ambito.com/contenidos/dolar-cl-historico.html'