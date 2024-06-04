import time
from selenium import webdriver
from selenium.webdriver.common.by import By


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

    def _extract_recent_data(self):
        try:
            # Esperar a que la página cargue completamente
            time.sleep(5)

            # Buscar el elemento con el dato usando XPath
            variacion_mensual_element = self.driver.find_element(By.XPATH, '/html/body/div[1]/div/div/main/article[1]/div[1]/h2')

            # Extraer el texto del elemento y guardarlo en la variable 'data_variacion_mensual'
            data_variacion_mensual = variacion_mensual_element.text

            # Buscar el elemento con el dato usando XPath
            variaciones_element = self.driver.find_element(By.XPATH, '/html/body/div[1]/div/div/main/article[1]/div[1]/p[1]')

            # Extraer el texto del elemento y guardarlo en la variable 'data_variaciones'
            data_variaciones = variaciones_element.text

            # Buscar el elemento con el dato usando XPath
            fecha = self.driver.find_element(By.XPATH, '/html/body/div[1]/div/div/main/article/header/h1')
            # Extraer el texto del elemento y guardarlo en la variable 'data_variaciones'
            data_fecha = fecha.text
            # Imprimir el dato por pantalla
            print(f"Dato fecha: {data_fecha}")
            print(f"Dato variacion mensual: {data_variacion_mensual}")
            print(f"Dato variaciones: {data_variaciones}")

            # Cerrar el navegador después de encontrar los dos datos
            self.driver.quit()

            return data_variacion_mensual  # Devolver el dato extraído

        except Exception as e:
            print(f"Error al extraer el dato: {e}")
            return None  # O algún valor predeterminado en caso de error

if __name__ == "__main__":
    extractor = readDataWebPage()
    extracted_data = extractor.extract_data()
