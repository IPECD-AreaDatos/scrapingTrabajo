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
        # Inicializamos con dtypes específicos para evitar el FutureWarning de concatenación
        self.df_total = pd.DataFrame({
            'fecha': pd.Series(dtype='datetime64[ns]'),
            'id_provincia': pd.Series(dtype='int64'),
            'id_vehiculo': pd.Series(dtype='int64'),
            'cantidad': pd.Series(dtype='int64')
        })
        self.driver = None
        self.original_window = None

    def extract(self) -> pd.DataFrame:
        """
        Extrae los datos de autos y motos del último año disponible.

        Returns:
            pd.DataFrame con columnas: fecha, id_provincia, id_vehiculo, cantidad
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
        # 1. Capturar todas las filas de la tabla
        filas = tabla.find_elements(By.TAG_NAME, "tr")
        
        datos = []
        for f in filas:
            # CAMBIO CLAVE: Capturar td y th para que no se desplacen las columnas
            celdas = f.find_elements(By.XPATH, ".//*[self::td or self::th]")
            fila_texto = [c.text.strip() for c in celdas]
            # Solo agregar si la fila tiene datos (evita filas vacías de diseño)
            if len(fila_texto) >= 13:
                datos.append(fila_texto)

        if not datos:
            logger.warning(f"[EXTRACT] No se encontraron datos en la tabla para tipo: {tipo_vehiculo}")
            return

        # Tomamos las filas de las provincias (suelen ser las primeras 24 con datos)
        # Y buscamos específicamente la que dice TOTAL
        df_temp = pd.DataFrame(datos)

        # 2. Diccionario de Mapeo de Provincias (Normalizado a minúsculas y sin puntos/espacios extra)
        MAPEO_PROVINCIAS = {
            "nacion": 1, "total": 1,
            "cautonomadebsas": 2, "caba": 2,
            "buenosaires": 6,
            "catamarca": 10,
            "cordoba": 14,
            "corrientes": 18,
            "chaco": 22,
            "chubut": 26,
            "entrerios": 30,
            "formosa": 34,
            "jujuy": 38,
            "lapampa": 42,
            "larioja": 46,
            "mendoza": 50,
            "misiones": 54,
            "neuquen": 58,
            "rionegro": 62,
            "salta": 66,
            "sanjuan": 70,
            "sanluis": 74,
            "santacruz": 78,
            "santafe": 82,
            "santiagodelestero": 86, "sgodelestero": 86,
            "tucuman": 90,
            "tierradelfuego": 94, "tdelfuego": 94
        }

        # Función local para normalizar los strings de las provincias de la web
        def normalizar_nombre(texto):
            if not texto:
                return ""
            return "".join(texto.lower().split()).replace(".", "").replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u")

       
        # Aplicamos la normalización a la columna de nombres de provincia
        df_temp['nombre_normalizado'] = df_temp[0].apply(normalizar_nombre)

        # Mapeamos los nombres para obtener el id_provincia real
        df_temp['id_provincia'] = df_temp['nombre_normalizado'].map(MAPEO_PROVINCIAS)

        # 3. Filtrado y Limpieza
        # Nos quedamos únicamente con las filas que logramos mapear exitosamente
        df_filtrado = df_temp[df_temp['id_provincia'].notna()].copy()

        # [MANEJO DE FILA FANTASMA DE CABA / DUPLICADOS]
        # Si hay filas duplicadas apuntando al mismo ID (como pasa en Motos con CABA), 
        # agrupamos por id_provincia sumando mes a mes para consolidar el dato real o descartar basura.
        # Primero convertimos las columnas de meses (1 a 12) a strings limpios para poder operar
        for col in range(1, 13):
            df_filtrado[col] = df_filtrado[col].str.replace(".", "", regex=False)
            df_filtrado[col] = pd.to_numeric(df_filtrado[col], errors='coerce').fillna(0).astype(int)

        # Agrupamos por id_provincia para consolidar filas repetidas si las hubiera
        df_consolidado = df_filtrado.groupby('id_provincia')[list(range(1, 13))].sum().reset_index()

        logger.info(f"[EXTRACT] Se procesaron y mapearon {len(df_consolidado)} entidades geográficas con éxito.")

        # 4. Preparar columnas de fecha para el Melt
        fechas = [datetime(int(valor_opcion), m, 1).strftime("%Y-%m-%d") for m in range(1, 13)]
        df_consolidado.columns = ['id_provincia'] + fechas

        # 5. Melt a formato transaccional
        df_melted = df_consolidado.melt(id_vars=['id_provincia'], var_name='fecha', value_name='cantidad')
        df_melted['id_vehiculo'] = tipo_vehiculo

        # Concatenamos al dataframe histórico de la instancia
        self.df_total = pd.concat([self.df_total, df_melted], ignore_index=True)
        
    def _transformar_cantidad_vehiculos(self):
        # Como 'cantidad' ya es entera, removemos el .str.replace y forzamos a numérico directamente
        self.df_total['id_provincia'] = self.df_total['id_provincia'].astype(int)
        self.df_total['id_vehiculo'] = self.df_total['id_vehiculo'].astype(int)
        self.df_total['cantidad'] = pd.to_numeric(self.df_total['cantidad'], errors='coerce').fillna(0).astype(int)
        self.df_total['fecha'] = pd.to_datetime(self.df_total['fecha'], format='%Y-%m-%d')
        
        # Mantener solo registros con movimientos
        self.df_total = self.df_total[self.df_total['cantidad'] > 0].reset_index(drop=True)