import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import re
from datetime import datetime
import pandas as pd

class readDataWebPage:
    def __init__(self):
        self.driver = None
        self.original_window = None

    def extract_data(self):
        self._setup_driver()
        self.driver.get('https://ipconlinebb.wordpress.com/')
        data = self._extract_recent_data()
        self.driver.quit()  # Close browser after extraction
        return data

    def _setup_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # Optional: Run Chrome headless
        self.driver = webdriver.Chrome(options=options)
        self.original_window = self.driver.current_window_handle

    def convertir_numerico(self, dato):
        dato = dato.replace({'%': '', ',': '.'}, regex=True)
        dato = float(dato)
        return dato


    def _extract_recent_data(self):
        try:
            # Esperar a que la página cargue completamente
            time.sleep(5)

            # Buscar el elemento con el dato usando XPath
            variacion_mensual_element = self.driver.find_element(By.XPATH, '/html/body/div[1]/div/div/main/article[1]/div[1]/h2')

            # Extraer el texto del elemento y guardarlo en la variable 'data_variacion_mensual'
            data_variacion_mensual = variacion_mensual_element.text

            data_variacion_mensual = re.sub(r'%|,', '', data_variacion_mensual)
            data_variacion_mensual = float(data_variacion_mensual)
            data_variacion_mensual /= 100


            # Buscar el elemento con el dato usando XPath
            variaciones_element = self.driver.find_element(By.XPATH, '/html/body/div[1]/div/div/main/article[1]/div[1]/p[1]')

            # Extraer el texto del elemento y guardarlo en la variable 'data_variaciones'
            data_variaciones = variaciones_element.text
            numeros = re.findall(r'\d+,\d+', data_variaciones)

            # Procesar los números encontrados
            numeros_procesados = [float(numero.replace(',', '.')) for numero in numeros]

            # Buscar el elemento con el dato usando XPath
            fecha = self.driver.find_element(By.XPATH, '/html/body/div[1]/div/div/main/article/header/h1')
            # Extraer el texto del elemento y guardarlo en la variable 'data_variaciones'
            data_fecha = fecha.text
            match = re.search(r'([A-Za-z]+) (\d{4})', data_fecha)

            if match:
                # Extraer el mes y el año
                nombre_mes = match.group(1).lower()  # Convertir el nombre del mes a minúsculas
                year = int(match.group(2))

                # Mapear el nombre del mes a su representación numérica
                meses = {
                    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5, "junio": 6,
                    "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10, "noviembre": 11, "diciembre": 12
                }

                if nombre_mes in meses:
                    # Construir la fecha con día fijo (1)
                    fecha_obj = datetime(year, meses[nombre_mes], 1).date()

                    print("Fecha extraída:", fecha_obj)
                else:
                    print("Nombre del mes no válido:", nombre_mes)
            else:
                print("No se encontró la fecha en el texto extraído.")

            # Imprimir el dato por pantalla
            print(f"Dato fecha: {fecha_obj}")
            print(type(fecha_obj))
            print(f"Dato variacion mensual: {data_variacion_mensual}")
            print(type(data_variacion_mensual))
            print(f"Cobertura IPCNu: {numeros_procesados[0]}")
            print(f"Variación acumulada interanual: {numeros_procesados[1]}")
            print(f"Variación acumulada en 2024: {numeros_procesados[2]}")

            df = pd.DataFrame({
                'fecha': [fecha_obj],
                'variacion_mensual': [data_variacion_mensual],
                'variacion_interanual_acumulada': [numeros_procesados[1]],
                'variacion_acumulada_del_año': [numeros_procesados[2]]
            })
            # Cerrar el navegador después de encontrar los dos datos
            self.driver.quit()

            return df  # Devolver el dato extraído

        except Exception as e:
            print(f"Error al extraer el dato: {e}")
            return None  # O algún valor predeterminado en caso de error

if __name__ == "__main__":
    extractor = readDataWebPage()
    extracted_data = extractor.extract_data()
