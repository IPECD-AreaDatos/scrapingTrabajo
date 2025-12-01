"""
Extractor para Ventas de Combustible
Descarga archivo CSV desde datos.gob.ar usando Selenium
"""
from typing import Any
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
from core.base_extractor import BaseExtractor
from config.settings import Settings


class VentasCombustibleExtractor(BaseExtractor):
    """Extractor para datos de ventas de combustible"""
    
    def __init__(self):
        super().__init__("ventas_combustible")
        self.settings = Settings()
        
        # Configurar Selenium
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        self.driver = webdriver.Chrome(options=options)
        
        # URL de la página
        self.url_pagina = (
            'https://datos.gob.ar/dataset/energia-refinacion-comercializacion-'
            'petroleo-gas-derivados-tablas-dinamicas/archivo/energia_f0e4e10a-e4b8-44e6-bd16-763a43742107'
        )
        
        # Directorio de archivos
        self.files_dir = Path(__file__).parent / 'files'
        self.files_dir.mkdir(exist_ok=True)
        self.output_file = self.files_dir / 'ventas_combustible.csv'
    
    def extract(self, **kwargs) -> dict:
        """
        Descarga el archivo CSV de ventas de combustible.
        
        Returns:
            Diccionario con información de la extracción:
            {
                'file_path': Path del archivo descargado,
                'data': None (el archivo se guarda en disco)
            }
        """
        try:
            # Cargar la página web
            self.logger.info(f"Accediendo a: {self.url_pagina}")
            self.driver.get(self.url_pagina)
            
            # Esperar y obtener el enlace del archivo
            wait = WebDriverWait(self.driver, 10)
            archivo_element = wait.until(
                EC.presence_of_element_located((By.XPATH, "/html/body/div[1]/div[2]/div/div/div[3]/a[1]"))
            )
            
            url_archivo = archivo_element.get_attribute('href')
            self.logger.info(f"URL del archivo encontrada: {url_archivo}")
            
            # Descargar el archivo
            self.logger.info(f"Descargando archivo a: {self.output_file}")
            response = requests.get(url_archivo, verify=False, timeout=60)
            response.raise_for_status()
            
            # Guardar el archivo
            with open(self.output_file, 'wb') as f:
                f.write(response.content)
            
            self.logger.info(f"Archivo guardado exitosamente: {self.output_file}")
            
            return {
                'file_path': str(self.output_file),
                'data': None,  # El archivo está en disco
            }
            
        except Exception as e:
            self.logger.error(f"Error durante la extracción: {e}")
            raise
    
    def cleanup(self) -> None:
        """Cierra el navegador Selenium"""
        try:
            if hasattr(self, 'driver') and self.driver:
                self.driver.quit()
                self.logger.debug("Navegador Selenium cerrado")
        except Exception as e:
            self.logger.warning(f"Error al cerrar navegador: {e}")



