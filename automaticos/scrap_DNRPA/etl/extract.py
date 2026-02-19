"""
EXTRACT - Módulo de extracción de datos DNRPA
Responsabilidad: Extraer datos de patentamiento (autos y motos) desde dnrpa.gov.ar usando Selenium
"""
import logging
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)

URL = 'https://www.dnrpa.gov.ar/portal_dnrpa/estadisticas/rrss_tramites/tram_prov.php?origen=portal_dnrpa&tipo_consulta=inscripciones'


class ExtractDNRPA:
    """Extrae datos de patentamiento del DNRPA (autos y motos del último año)."""

    def __init__(self):
        self.df_total = pd.DataFrame(columns=['fecha', 'id_provincia_indec', 'id_vehiculo', 'cantidad'])
        self.driver = None
        self.original_window = None

    def extract(self) -> pd.DataFrame:
        """
        Extrae los datos de autos y motos del último año disponible.

        Returns:
            pd.DataFrame con columnas: fecha, id_provincia_indec, id_vehiculo, cantidad
        """
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        self.driver = webdriver.Chrome(options=options)
        self.original_window = self.driver.current_window_handle

        try:
            logger.info("[EXTRACT] Navegando a DNRPA: %s", URL)
            self.driver.get(URL)
            self._buscar_datos_de_tabla(1)  # Autos
            self._buscar_datos_de_tabla(2)  # Motos
            self._transformar_cantidad_vehiculos()
            logger.info("[EXTRACT] DNRPA: %d filas extraídas", len(self.df_total))
        finally:
            self.driver.quit()

        return self.df_total

    def _buscar_datos_de_tabla(self, tipo_vehiculo: int):
        elemento = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.XPATH,
                '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[2]/td/select'))
        )
        opciones = elemento.find_elements(By.TAG_NAME, 'option')
        opciones.reverse()
        anos = [int(o.text) for o in opciones if o.text.isdigit()]
        ultimo_anio = max(anos)

        if tipo_vehiculo == 1:
            radio = self.driver.find_element(By.XPATH,
                '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[4]/td/input[1]')
        else:
            radio = self.driver.find_element(By.XPATH,
                '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/table/tbody/tr[4]/td/input[2]')

        button = self.driver.find_element(By.XPATH,
            '/html/body/div[2]/section/article/div/div[2]/div/div/div/div/form/div/center/center/input')

        try:
            opcion = elemento.find_element(By.XPATH, '//*[@id="seleccion"]/center/table/tbody/tr[2]/td/select/option[2]')
            opcion.click()
        except NoSuchElementException:
            logger.warning("[EXTRACT] No se encontró opción de año %s", ultimo_anio)
            return

        radio.click()
        button.click()

        windows = self.driver.window_handles
        self.driver.switch_to.window(windows[-1])

        tabla = self.driver.find_element(By.XPATH, '//*[@id="seleccion"]/div/table')
        self._construir_tabla(tabla, str(ultimo_anio), tipo_vehiculo)

        self.driver.close()
        self.driver.switch_to.window(self.original_window)

    def _construir_tabla(self, tabla, valor_opcion: str, tipo_vehiculo: int):
        filas = tabla.find_elements(By.TAG_NAME, "tr")
        datos = [[c.text for c in f.find_elements(By.TAG_NAME, "td")] for f in filas]
        df = pd.DataFrame(datos).iloc[2:26, 0:13]
        df[df.columns[0]] = [6, 2, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46, 50, 54, 58, 62, 66, 70, 74, 78, 82, 86, 90, 94]
        fechas = [datetime(int(valor_opcion), m, 1).strftime("%Y-%m-%d") for m in range(1, 13)]
        df.columns = ['id_provincia_indec'] + fechas
        df_melted = df.melt(id_vars=['id_provincia_indec'], var_name='fecha', value_name='cantidad')
        df_melted['id_vehiculo'] = tipo_vehiculo
        self.df_total = pd.concat([self.df_total, df_melted])

    def _transformar_cantidad_vehiculos(self):
        self.df_total['cantidad'] = self.df_total['cantidad'].str.replace(".", "", regex=False)
        self.df_total['id_provincia_indec'] = self.df_total['id_provincia_indec'].astype(int)
        self.df_total['id_vehiculo'] = self.df_total['id_vehiculo'].astype(int)
        self.df_total['cantidad'] = pd.to_numeric(self.df_total['cantidad'], errors='coerce').fillna(0).astype(int)
        self.df_total['fecha'] = pd.to_datetime(self.df_total['fecha'], format='%Y-%m-%d')
        self.df_total = self.df_total[self.df_total['cantidad'] > 0]
