import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
import os
import urllib3

class downloadArchive:
    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        self.driver = webdriver.Chrome(options=options)

        self.base_urls = {
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


    def descargar_archivo(self):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')
        os.makedirs(base_path, exist_ok=True)

        for provincia, url in self.base_urls.items():
            print(f"Descargando archivos de {provincia}...")
            self.driver.get(url)
            wait = WebDriverWait(self.driver, 10)

            # Crear carpeta por provincia
            path_provincia = os.path.join(base_path, provincia)
            os.makedirs(path_provincia, exist_ok=True)

            try:
                archivo1 = wait.until(EC.presence_of_element_located((By.XPATH, "//div[3]/div[2]/div[1]/div[1]/a")))
                url_archivo1 = archivo1.get_attribute('href')
                response1 = requests.get(url_archivo1, verify=False)
                with open(os.path.join(path_provincia, 'empresas_en_actividad.xls'), 'wb') as f:
                    f.write(response1.content)
            except Exception as e:
                print(f"❌ Error descargando archivo 1 para {provincia}: {e}")

            try:
                archivo2 = wait.until(EC.presence_of_element_located((By.XPATH, "//div[3]/div[2]/div[2]/div[1]/a")))
                url_archivo2 = archivo2.get_attribute('href')
                response2 = requests.get(url_archivo2, verify=False)
                with open(os.path.join(path_provincia, 'puestos_de_trabajo_registrados.xls'), 'wb') as f:
                    f.write(response2.content)
            except Exception as e:
                print(f"❌ Error descargando archivo 2 para {provincia}: {e}")

            try:
                archivo3 = wait.until(EC.presence_of_element_located(
                    (By.XPATH, "//a[contains(@href, 'Salario-Promedio')]")
                ))
                url_archivo3 = archivo3.get_attribute('href')
                response3 = requests.get(url_archivo3, verify=False)

                with open(os.path.join(path_provincia, 'salario_promedio_construccion.xls'), 'wb') as f:
                    f.write(response3.content)
            except Exception as e:
                print(f"❌ Error descargando archivo 3 (Salario Promedio) para {provincia}: {e}")

