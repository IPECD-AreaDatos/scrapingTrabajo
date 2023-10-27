from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import gspread

class read:
    def sheets(self):
        driver = webdriver.Chrome()
        # URL de la página que deseas obtener
        url_pagina = 'https://docs.google.com/spreadsheets/d/1NGcF5fXO7RCXIRGJ2UQO98x_T_tZtwHTnvD-RmTdV0E/edit?usp=sharing'

        # Abre la página
        driver.get(url_pagina)

        # Espera a que un elemento específico se cargue (puedes cambiar 'element_id' por el ID del elemento que deseas esperar)
        element_id = 'docs-menubar'
        wait = WebDriverWait(driver, 10)  # 10 segundos de tiempo de espera (ajusta según sea necesario)
        wait.until(EC.presence_of_element_located((By.ID, element_id)))
        
        # Descargar los datos de Google Sheets usando gspread y pandas
        gc = gspread.service_account()
        worksheet = gc.open_by_url(url_pagina).sheet1  # Hoja 1 de la hoja de cálculo
        values = worksheet.get_all_values()

        # Crea un DataFrame de Pandas a partir de los valores
        df = pd.DataFrame(values)

        # Opcional: Establece la primera fila como encabezado
        df.columns = df.iloc[0]
        df = df[1:]
        print(df)
        
read().sheets()
                