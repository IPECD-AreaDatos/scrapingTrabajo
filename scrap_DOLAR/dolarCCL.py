from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import pandas as pd
import urllib3
import time
from datetime import datetime
import pandas as pd
import time
import mysql.connector

nuevos_datos = []

class dolarCCL:
    def tomaDolarCCL(self, host, user, password,database):
        self.url_ccl = 'https://www.ambito.com/contenidos/dolar-cl-historico.html'
        self.driver = webdriver.Chrome()  # Reemplaza con la ubicación de tu ChromeDriver
        self.driver.maximize_window()
        
        # Desactivar advertencias de solicitud no segura
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.driver.get(self.url_ccl)
        time.sleep(10)
        
        wait = WebDriverWait(self.driver, 30)
        
        popup = wait.until(EC.element_to_be_clickable((By.ID, "onesignal-slidedown-cancel-button")))
        self.driver.execute_script("arguments[0].scrollIntoView();", popup)
        popup.click()
        
        # Esperar a que el iframe desaparezca
        if self.driver.find_elements(By.ID, "google_ads_iframe_/78858960/Ambito/Not-HE_0"):
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
        
        # Ahora, hacer clic en el botón
        boton = self.driver.find_element(By.CLASS_NAME, "general-historical__button")
        boton.click()
        time.sleep(20)
        table = self.driver.find_element(By.CLASS_NAME, 'general-historical__table')

        # Obtener el HTML de la tabla
        table_html = table.get_attribute('outerHTML')

        # Reemplazar comas por puntos en los datos
        table_html = table_html.replace(',', '.')

        # Leer la tabla HTML en un DataFrame de pandas
        df = pd.read_html(table_html)[0]
        df['Fecha'] = df['Fecha'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').strftime('%Y-%m-%d'))
        print(df)

        try:
            conn = mysql.connector.connect(
                host=host, user=user, password=password, database=database
            )
            cursor = conn.cursor()
            
            table_name = 'dolar_ccl'
            
            longitud_datos_excel = len(df)
            print("Longitud Datos Dolar Blue: ", longitud_datos_excel)

            select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
            cursor.execute(select_row_count_query)
            filas_BD = cursor.fetchone()[0]
            print("Base: ", filas_BD)

            if longitud_datos_excel != filas_BD:
                df_datos_nuevos = df.tail(longitud_datos_excel - filas_BD)

                print("Dolar CCL")
                insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in range(len(df.columns))])})"

                for index, row in df_datos_nuevos.iterrows():
                    data_tuple = tuple(row)
                    cursor.execute(insert_query, data_tuple)
                    print(data_tuple)

                conn.commit()
                print("Se agregaron nuevos datos")
            else:
                print("Se realizó una verificación de la base de datos")

        except mysql.connector.Error as err:
            print(f"Error MySQL: {err}")

        finally:
            if 'conn' in locals() and conn.is_connected():
                cursor.close()
                conn.close()
