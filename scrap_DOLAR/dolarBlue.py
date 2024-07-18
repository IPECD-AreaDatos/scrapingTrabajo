from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import urllib3
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import mysql.connector
from io import StringIO
import time

class dolarBlue:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.url_blue = 'https://www.ambito.com/contenidos/dolar-informal-historico.html'
        self.setup_driver()
        self.fecha_actual = datetime.now().strftime("%d/%m/%Y")

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
        except Exception as e:
            print(f"No se encontró el popup de notificaciones o no se pudo cerrar: {e}")
        try:
            ad_close_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//div[contains(@class, 'close-button')]")))
            ad_close_button.click()
        except Exception as e:
            print(f"No se encontró la publicidad emergente o no se pudo cerrar: {e}")

    def wait_for_element(self, by, identifier):
        wait = WebDriverWait(self.driver, 30)
        return wait.until(EC.visibility_of_element_located((by, identifier)))

    def tomaDolarBlue(self):
        try:
            self.driver.get(self.url_blue)
            print("Página cargada con éxito")
            
            # Cerrar popups de notificaciones y publicidad
            try:
                self.close_popups()
            except Exception as e:
                print(f"No se encontró el popup de notificaciones o no se pudo cerrar: {e}")
            
            # Encontrar los campos de fecha y el botón de búsqueda
            try:
                fecha_desde = self.wait_for_element(By.CLASS_NAME, "general-historical__datepicker.datepicker.desde.form-control")
                fecha_hasta = self.wait_for_element(By.CLASS_NAME, "general-historical__datepicker.datepicker.hasta.form-control")
                boton = self.wait_for_element(By.CLASS_NAME, "general-historical__button")
                print("Campos de fecha y botón encontrados")
            except Exception as e:
                print(f"No se encontraron los campos de fecha o el botón: {e}")
                return
            
            # Crear un DataFrame vacío para almacenar los datos
            df_total = pd.DataFrame(columns=['fecha', 'compra', 'venta'])

            # Usar solo la fecha actual
            dia_actual = self.fecha_actual
            fecha_desde.clear()
            fecha_desde.send_keys(dia_actual)
            fecha_hasta.clear()
            fecha_hasta.send_keys(dia_actual)

            try:
                boton.click()
                print(f"Botón clicado para la fecha: {dia_actual}")
            except Exception as e:
                print(f"Elemento click interceptado para la fecha {dia_actual}, intentando desplazarse y hacer clic nuevamente: {e}")
                try:
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", boton)
                    WebDriverWait(self.driver, 5).until(EC.element_to_be_clickable((By.CLASS_NAME, "general-historical__button")))
                    boton.click()
                    print(f"Botón clicado después de desplazar para la fecha: {dia_actual}")
                except Exception as e:
                    print(f"No se pudo hacer clic en el botón para la fecha {dia_actual}: {e}")
                    return

            # Esperar a que la tabla se cargue
            try:
                time.sleep(3)
                table = WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.CLASS_NAME, 'general-historical__table'))
                )
                print(f"Tabla encontrada para la fecha: {dia_actual}")
            except Exception as e:
                print(f"No se encontró la tabla para la fecha {dia_actual}: {e}")
                return

            # Verificar el contenido de la tabla antes de leerla
            try:
                table_html = table.get_attribute('outerHTML').replace(',', '.')
                df = pd.read_html(StringIO(table_html))[0]
                if not df.empty:
                    if len(df.columns) == 4:
                        df = df.iloc[:, 1:4]
                    df['fecha'] = datetime.strptime(dia_actual, "%d/%m/%Y").strftime('%Y-%m-%d')  # Convertir la fecha al formato YYYY-MM-DD
                    df.columns = ['fecha', 'compra', 'venta']
                    print(df)
                    df_total = pd.concat([df_total, df], ignore_index=True)
                else:
                    print(f"No hay datos disponibles para la fecha {dia_actual}")
                    return
            except Exception as e:
                print(f"Error al leer la tabla HTML para la fecha {dia_actual}: {e}")
                return

            # Verificar la última fecha en la base de datos
            if self.check_last_date(df_total):
                # Actualizar la base de datos con el DataFrame total
                self.update_database(df_total)
            else:
                print("La fecha actual ya está presente en la base de datos o no es posterior a la última fecha")

        except Exception as e:
            print(f"Error al obtener datos del dólar blue: {e}")
        finally:
            self.driver.quit()

    def check_last_date(self, df):
        try:
            engine = create_engine(f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}')
            with engine.connect() as connection:
                last_date_query = text("SELECT MAX(fecha) FROM dolar_blue")
                result = connection.execute(last_date_query).scalar()
                if result:
                    last_date = datetime.strptime(result, '%Y-%m-%d')
                    current_date = datetime.strptime(df['fecha'].iloc[0], '%Y-%m-%d')
                    return current_date > last_date
                return True
        except mysql.connector.Error as err:
            print(f"Error MySQL: {err}")
            return False
        finally:
            engine.dispose()

    def update_database(self, df):
        try:
            engine = create_engine(f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}')
            table_name = 'dolar_blue'

            # Insertar los nuevos datos sin truncar la tabla
            df.to_sql(table_name, con=engine, if_exists='append', index=False)
            print("Se actualizaron los datos en la base de datos")

        except mysql.connector.Error as err:
            print(f"Error MySQL: {err}")

        finally:
            engine.dispose()

