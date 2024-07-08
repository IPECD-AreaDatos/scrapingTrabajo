from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import urllib3
from datetime import datetime
from sqlalchemy import create_engine, text
import mysql.connector
from io import StringIO

class dolarBlue:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.url_blue = 'https://www.ambito.com/contenidos/dolar-informal-historico.html'
        self.setup_driver()

    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--start-maximized")
        #chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu") 
        self.driver = webdriver.Chrome(options=chrome_options) 
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def close_popups(self):
        wait = WebDriverWait(self.driver, 30)
        try:
            popup = wait.until(EC.element_to_be_clickable((By.ID, "onesignal-slidedown-cancel-button")))
            popup.click()
        except Exception:
            print("No se encontró el popup de notificaciones o no se pudo cerrar.")
        try:
            ad_close_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'close-button')]")))
            ad_close_button.click()
        except Exception:
            print("No se encontró la publicidad emergente o no se pudo cerrar.")

    def wait_for_element(self, by, identifier):
        wait = WebDriverWait(self.driver, 30)
        return wait.until(EC.visibility_of_element_located((by, identifier)))

    def tomaDolarBlue(self):
        try:
            self.driver.get(self.url_blue)
            self.close_popups()

            fecha_desde = self.wait_for_element(By.CLASS_NAME, "general-historical__datepicker.datepicker.desde.form-control")
            fecha_hasta = self.wait_for_element(By.CLASS_NAME, "general-historical__datepicker.datepicker.hasta.form-control")

            fecha_actual = datetime.now()
            dia_anterior = f"{fecha_actual.day}/{fecha_actual.month}/{fecha_actual.year}"

            fecha_desde.clear()
            fecha_desde.send_keys("01/01/2003")
            fecha_hasta.clear()
            fecha_hasta.send_keys(dia_anterior)

            boton = self.wait_for_element(By.CLASS_NAME, "general-historical__button")

            # Intentar hacer clic en el botón, manejando posibles elementos que intercepten el clic
            try:
                boton.click()
            except Exception:
                print("Elemento click interceptado, intentando desplazarse y hacer clic nuevamente...")
                self.driver.execute_script("arguments[0].scrollIntoView(true);", boton)
                WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, "general-historical__button")))
                boton.click()

            table = self.wait_for_element(By.CLASS_NAME, 'general-historical__table')
            table_html = table.get_attribute('outerHTML').replace(',', '.')

            # Usar StringIO para manejar FutureWarning
            df = pd.read_html(StringIO(table_html))[0]
            df['Fecha'] = df['Fecha'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y').strftime('%Y-%m-%d'))
            print(df)

            self.update_database(df)

        except Exception as e:
            print(f"Se produjo un error: {e}")

        finally:
            self.driver.quit()

    def update_database(self, df):
        try:
            engine = create_engine(f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}')

            table_name = 'dolar_blue'

            # Leer los datos actuales de la base de datos
            df_existing = pd.read_sql(f'SELECT * FROM {table_name}', con=engine)

            if not df.equals(df_existing):
                with engine.connect() as connection:
                    # Truncar la tabla
                    connection.execute(text(f'TRUNCATE TABLE {table_name}'))
                    # Cargar el nuevo DataFrame
                    df.to_sql(table_name, con=engine, if_exists='append', index=False)
                print("Se actualizaron los datos en la base de datos")
            else:
                print("No hay nuevos datos para actualizar")

        except mysql.connector.Error as err:
            print(f"Error MySQL: {err}")

        finally:
            engine.dispose()