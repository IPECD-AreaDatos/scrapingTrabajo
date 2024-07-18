import pandas as pd
import urllib3
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import mysql.connector
from io import StringIO
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

class dolarBlue:
    def __init__(self, host, user, password, database, fecha_inicio, fecha_fin):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.url_blue = 'https://www.ambito.com/contenidos/dolar-informal-historico.html'
        self.fecha_inicio = datetime.strptime(fecha_inicio, "%d/%m/%Y")
        self.fecha_fin = datetime.strptime(fecha_fin, "%d/%m/%Y")
        self.setup_driver()

    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--start-maximized")
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
        fecha_actual = self.fecha_inicio

        while fecha_actual <= self.fecha_fin:
            fecha_siguiente = fecha_actual + timedelta(days=1)
            try:
                self.driver.get(self.url_blue)

                # Cerrar popups de notificaciones y publicidad
                self.close_popups()

                # Encontrar y rellenar las fechas
                fecha_desde = self.wait_for_element(By.CLASS_NAME, "general-historical__datepicker.datepicker.desde.form-control")
                fecha_hasta = self.wait_for_element(By.CLASS_NAME, "general-historical__datepicker.datepicker.hasta.form-control")

                fecha_desde.clear()
                fecha_desde.send_keys(fecha_actual.strftime("%d/%m/%Y"))
                fecha_hasta.clear()
                fecha_hasta.send_keys(fecha_siguiente.strftime("%d/%m/%Y"))

                boton = self.wait_for_element(By.CLASS_NAME, "general-historical__button")
                
                # Intentar hacer clic en el botón, manejando posibles elementos que intercepten el clic
                try:
                    boton.click()
                except Exception:
                    print("Elemento click interceptado, intentando desplazarse y hacer clic nuevamente...")
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", boton)
                    WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, "general-historical__button")))
                    boton.click()

                # Esperar explícitamente hasta que la tabla esté presente y se haya actualizado
                table = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'general-historical__table'))
                )

                # Esperar hasta que la tabla contenga al menos un dato en el tbody
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, '.general-historical__tbody tbody tr'))
                )

                table_html = table.get_attribute('outerHTML').replace(',', '.')

                # Usar StringIO para manejar FutureWarning
                df = pd.read_html(StringIO(table_html))[0]
                df.columns = ['fecha', 'compra', 'venta']  # Asegurar que las columnas tienen los nombres correctos
                df['fecha'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')

                # Insertar los datos en la base de datos
                self.update_database(df)
                print(f"Datos obtenidos y actualizados para la fecha: {fecha_actual.strftime('%d/%m/%Y')}")

            except Exception as e:
                print(f"Error al obtener datos para la fecha: {fecha_actual.strftime('%d/%m/%Y')} - {e}")

            fecha_actual = fecha_siguiente

    def update_database(self, df):
        try:
            engine = create_engine(f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}')

            table_name = 'dolar_blue'

            with engine.connect() as connection:
                for index, row in df.iterrows():
                    query = text(f"""
                    INSERT INTO {table_name} (fecha, compra, venta)
                    VALUES (:fecha, :compra, :venta)
                    ON DUPLICATE KEY UPDATE
                        compra = VALUES(compra),
                        venta = VALUES(venta)
                    """)
                    connection.execute(query, {
                        'fecha': row['fecha'],
                        'compra': row['compra'],
                        'venta': row['venta']
                    })
            print("Datos actualizados en la base de datos")

        except mysql.connector.Error as err:
            print(f"Error MySQL: {err}")

        finally:
            engine.dispose()
