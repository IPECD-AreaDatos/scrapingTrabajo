"""
EXTRACT - Módulo de extracción de datos IERIC
Responsabilidad: Descargar 3 XLS por provincia desde ieric.org.ar usando Selenium
"""
import os
import logging
import requests
import urllib3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'files')

BASE_URLS = {
    "buenos_aires": "https://www.ieric.org.ar/series_estadisticas/buenos-aires/",
    "catamarca": "https://www.ieric.org.ar/series_estadisticas/catamarca/",
    "chaco": "https://www.ieric.org.ar/series_estadisticas/chaco/",
    "chubut": "https://www.ieric.org.ar/series_estadisticas/chubut/",
    "cordoba": "https://www.ieric.org.ar/series_estadisticas/cordoba/",
    "corrientes": "https://www.ieric.org.ar/series_estadisticas/corrientes/",
    "entre_rios": "https://www.ieric.org.ar/series_estadisticas/entre-rios/",
    "formosa": "https://www.ieric.org.ar/series_estadisticas/formosa/",
    "jujuy": "https://www.ieric.org.ar/series_estadisticas/jujuy/",
    "la_pampa": "https://www.ieric.org.ar/series_estadisticas/la-pampa/",
    "la_rioja": "https://www.ieric.org.ar/series_estadisticas/la-rioja/",
    "mendoza": "https://www.ieric.org.ar/series_estadisticas/mendoza/",
    "misiones": "https://www.ieric.org.ar/series_estadisticas/misiones/",
    "neuquen": "https://www.ieric.org.ar/series_estadisticas/neuquen/",
    "rio_negro": "https://www.ieric.org.ar/series_estadisticas/rio-negro/",
    "salta": "https://www.ieric.org.ar/series_estadisticas/salta/",
    "san_juan": "https://www.ieric.org.ar/series_estadisticas/san-juan/",
    "san_luis": "https://www.ieric.org.ar/series_estadisticas/san-luis/",
    "santa_cruz": "https://www.ieric.org.ar/series_estadisticas/santa-cruz/",
    "santa_fe": "https://www.ieric.org.ar/series_estadisticas/santa-fe/",
    "santiago_del_estero": "https://www.ieric.org.ar/series_estadisticas/santiago-del-estero/",
    "tierra_del_fuego": "https://www.ieric.org.ar/series_estadisticas/tierra-del-fuego/",
    "tucuman": "https://www.ieric.org.ar/series_estadisticas/tucuman/",
}


class ExtractIERIC:
    """Descarga los 3 XLS de cada provincia del IERIC."""

    def extract(self) -> str:
        """
        Descarga todos los archivos y retorna el directorio files/.

        Returns:
            str: Ruta al directorio files/
        """
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        os.makedirs(FILES_DIR, exist_ok=True)

        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        driver = webdriver.Chrome(options=options)

        try:
            for provincia, url in BASE_URLS.items():
                logger.info("[EXTRACT] Descargando provincia: %s", provincia)
                driver.get(url)
                wait = WebDriverWait(driver, 10)
                path_prov = os.path.join(FILES_DIR, provincia)
                os.makedirs(path_prov, exist_ok=True)

                self._descargar_archivo(driver, wait, path_prov,
                    "//div[3]/div[2]/div[1]/div[1]/a", 'empresas_en_actividad.xls', provincia)
                self._descargar_archivo(driver, wait, path_prov,
                    "//div[3]/div[2]/div[2]/div[1]/a", 'puestos_de_trabajo_registrados.xls', provincia)
                self._descargar_archivo(driver, wait, path_prov,
                    "//a[contains(@href, 'Salario-Promedio')]", 'salario_promedio_construccion.xls', provincia)
        finally:
            driver.quit()

        logger.info("[EXTRACT] Descarga IERIC completada. Directorio: %s", FILES_DIR)
        return FILES_DIR

    def _descargar_archivo(self, driver, wait, path_prov, xpath, nombre, provincia):
        try:
            elem = wait.until(EC.presence_of_element_located((By.XPATH, xpath)))
            url  = elem.get_attribute('href')
            resp = requests.get(url, verify=False, timeout=30)
            with open(os.path.join(path_prov, nombre), 'wb') as f:
                f.write(resp.content)
        except Exception as e:
            logger.warning("[EXTRACT] Error descargando '%s' para %s: %s", nombre, provincia, e)
