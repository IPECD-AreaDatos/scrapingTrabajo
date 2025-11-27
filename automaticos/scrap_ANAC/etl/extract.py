"""
EXTRACT - Módulo de extracción de datos ANAC
Responsabilidad: Descargar y extraer archivos desde la fuente

NOTA: Actualmente usa requests (no requiere Selenium).
Si en el futuro se necesita Selenium, usar modo headless para Linux.
"""
import requests
import os
import urllib3
import zipfile
import shutil
import logging

logger = logging.getLogger(__name__)

class ExtractANAC:
    """Clase para extraer datos de ANAC"""
    
    def __init__(self, use_selenium=False, headless=True):
        """
        Inicializa la clase para descargar archivos de ANAC
        
        Args:
            use_selenium: Si True, usa Selenium (por defecto False, usa requests)
            headless: Si True y use_selenium=True, ejecuta en modo headless (para Linux)
        """
        self.url_base = 'https://datos.anac.gob.ar/estadisticas/'
        self.url_descarga = 'https://docs.anac.gob.ar/index.php/s/4ptegdXanm2rWJG/download'
        # Directorio del módulo (etl/) y carpeta files en el directorio padre
        self.directorio_actual = os.path.dirname(os.path.abspath(__file__))
        self.directorio_proyecto = os.path.dirname(self.directorio_actual)  # Directorio scrap_ANAC
        self.carpeta_files = os.path.join(self.directorio_proyecto, 'files')
        self.nombre_archivo_zip = 'anac_estadisticas.zip'
        self.nombre_archivo_excel = 'anac.xlsx'
        self.use_selenium = use_selenium
        self.headless = headless
        self.driver = None

    def extract(self):
        """
        Descarga el archivo comprimido de estadísticas de la ANAC y lo descomprime
        
        Returns:
            str: Ruta completa al archivo Excel extraído
        """
        try:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
            # Crear carpeta files si no existe
            if not os.path.exists(self.carpeta_files):
                os.makedirs(self.carpeta_files)
                logger.info(f"Carpeta creada: {self.carpeta_files}")

            ruta_zip = os.path.join(self.carpeta_files, self.nombre_archivo_zip)
            ruta_excel = os.path.join(self.carpeta_files, self.nombre_archivo_excel)

            # Verificar espacio libre antes de descargar
            self._verificar_espacio_disco()
            
            # Verificar si el archivo Excel ya existe y es reciente (menos de 1 hora)
            if os.path.exists(ruta_excel):
                import time
                tiempo_archivo = os.path.getmtime(ruta_excel)
                tiempo_actual = time.time()
                if (tiempo_actual - tiempo_archivo) < 3600:  # 1 hora
                    logger.info(f"[OK] Usando archivo Excel existente (descargado hace menos de 1 hora)")
                    return ruta_excel

            # Descargar el archivo ZIP con reintentos
            logger.info(f"Descargando archivo comprimido desde: {self.url_descarga}")
            self._descargar_zip(ruta_zip)

            # Extraer archivo Excel
            logger.info("Descomprimiendo archivo...")
            self._extraer_excel(ruta_zip, ruta_excel)

            # Limpiar archivos temporales
            self._limpiar_archivos_temporales(ruta_zip)
            
            logger.info(f"[OK] Archivo Excel extraído exitosamente: {ruta_excel}")
            return ruta_excel

        except Exception as e:
            logger.error(f"Error en extracción: {e}")
            raise
        finally:
            # Cerrar driver de Selenium si se usó
            if self.driver:
                try:
                    self.driver.quit()
                    self.driver = None
                except:
                    pass

    def _descargar_zip(self, ruta_zip):
        """
        Descarga el archivo ZIP con reintentos
        
        Si use_selenium=True, usa Selenium (útil si la descarga requiere JavaScript).
        Si use_selenium=False, usa requests (más rápido, funciona en Linux sin problemas).
        """
        if self.use_selenium:
            return self._descargar_zip_selenium(ruta_zip)
        else:
            return self._descargar_zip_requests(ruta_zip)
    
    def _descargar_zip_requests(self, ruta_zip):
        """Descarga el archivo ZIP usando requests (método actual, funciona en Linux)"""
        max_intentos = 3
        for intento in range(max_intentos):
            try:
                response = requests.get(self.url_descarga, verify=False, timeout=60)
                if response.status_code == 200:
                    with open(ruta_zip, 'wb') as file:
                        file.write(response.content)
                    logger.info(f"[OK] Archivo comprimido descargado: {ruta_zip} ({len(response.content)} bytes)")
                    return
                else:
                    raise Exception(f"Error HTTP {response.status_code}")
            except requests.exceptions.RequestException as e:
                if intento < max_intentos - 1:
                    logger.warning(f"Intento {intento + 1} falló: {e}. Reintentando...")
                    continue
                else:
                    raise Exception(f"Error después de {max_intentos} intentos: {e}")
    
    def _descargar_zip_selenium(self, ruta_zip):
        """
        Descarga el archivo ZIP usando Selenium (para casos que requieren JavaScript)
        Configurado para funcionar en Linux con modo headless
        """
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            from selenium.webdriver.chrome.service import Service
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            import time
            
            # Configurar Chrome para Linux (headless)
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument('--headless')  # Sin ventana (para Linux)
                chrome_options.add_argument('--no-sandbox')  # Necesario para Linux
                chrome_options.add_argument('--disable-dev-shm-usage')  # Evita problemas de memoria en Linux
                chrome_options.add_argument('--disable-gpu')  # No necesita GPU en servidor
                logger.info("Configurando Selenium en modo headless (para Linux)")
            
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36')
            
            # Buscar chromedriver (puede estar en la carpeta selenium del proyecto)
            chromedriver_path = None
            possible_paths = [
                os.path.join(self.directorio_actual, 'selenium', 'chromedriver'),
                os.path.join(self.directorio_actual, '..', '..', 'selenium', 'chromedriver'),
                '/usr/local/bin/chromedriver',
                '/usr/bin/chromedriver',
            ]
            
            for path in possible_paths:
                if os.path.exists(path):
                    chromedriver_path = path
                    break
            
            if chromedriver_path:
                service = Service(chromedriver_path)
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            else:
                # Intentar usar chromedriver del PATH
                self.driver = webdriver.Chrome(options=chrome_options)
            
            logger.info(f"Descargando con Selenium desde: {self.url_descarga}")
            self.driver.get(self.url_descarga)
            
            # Esperar a que se descargue (ajustar según sea necesario)
            time.sleep(5)
            
            # Si hay un botón de descarga, hacer clic
            try:
                download_button = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "a"))
                )
                download_button.click()
                time.sleep(10)  # Esperar descarga
            except:
                pass
            
            # Buscar el archivo descargado en la carpeta de descargas
            # (implementar lógica según sea necesario)
            logger.warning("[WARNING] Descarga con Selenium no completamente implementada")
            logger.info("💡 Considera usar requests (use_selenium=False) que funciona mejor en Linux")
            
            # Por ahora, lanzar excepción indicando que se debe usar requests
            raise NotImplementedError(
                "Descarga con Selenium no está completamente implementada. "
                "Usa use_selenium=False para usar requests (recomendado para Linux)."
            )
            
        except ImportError:
            logger.error("Selenium no está instalado. Instala con: pip install selenium")
            raise
        except Exception as e:
            logger.error(f"Error en descarga con Selenium: {e}")
            raise

    def _extraer_excel(self, ruta_zip, ruta_excel_final):
        """Extrae el archivo Excel del ZIP"""
        try:
            with zipfile.ZipFile(ruta_zip, 'r') as zf:
                contenidos = zf.namelist()
                logger.debug(f"Archivos en el ZIP: {contenidos}")
                
                # Buscar archivo Excel más reciente (2023-2025)
                archivos_excel = [a for a in contenidos if a.lower().endswith(('.xlsx', '.xls'))]
                
                archivo_excel = None
                for archivo in archivos_excel:
                    if '2023-2025' in archivo:
                        archivo_excel = archivo
                        break
                
                if archivo_excel is None and archivos_excel:
                    archivo_excel = archivos_excel[0]
                
                if not archivo_excel:
                    raise Exception("No se encontró ningún archivo Excel en el ZIP")
                
                logger.info(f"Archivo Excel encontrado: {archivo_excel}")
                zf.extract(archivo_excel, self.carpeta_files)
                
                # Renombrar si es necesario
                ruta_excel_extraido = os.path.join(self.carpeta_files, archivo_excel)
                if ruta_excel_extraido != ruta_excel_final:
                    shutil.move(ruta_excel_extraido, ruta_excel_final)
                    logger.info(f"Archivo renombrado a: {ruta_excel_final}")
                    
        except zipfile.BadZipFile as e:
            logger.error(f"Error: El archivo descargado no es un ZIP válido: {e}")
            raise

    def _limpiar_archivos_temporales(self, ruta_zip):
        """Limpia archivos temporales y directorios extraídos"""
        archivos_eliminados = 0
        espacio_liberado = 0
        
        try:
            # Eliminar ZIP temporal
            if os.path.exists(ruta_zip):
                size = os.path.getsize(ruta_zip)
                os.remove(ruta_zip)
                espacio_liberado += size
                archivos_eliminados += 1
                logger.info(f"[OK] ZIP temporal eliminado ({size/1024/1024:.1f}MB)")
            
            # Eliminar directorio de extracción temporal
            directorio_temporal = os.path.join(self.carpeta_files, "tablas de movimientos y pasajeros")
            if os.path.exists(directorio_temporal):
                for root, dirs, files in os.walk(directorio_temporal):
                    for file in files:
                        filepath = os.path.join(root, file)
                        if os.path.exists(filepath):
                            espacio_liberado += os.path.getsize(filepath)
                            archivos_eliminados += 1
                shutil.rmtree(directorio_temporal)
                logger.info(f"[OK] Directorio temporal eliminado")
            
            if archivos_eliminados > 0:
                logger.info(f"[OK] Limpieza completada: {archivos_eliminados} archivos, {espacio_liberado/1024/1024:.1f}MB liberados")
                
        except PermissionError:
            logger.warning("[WARNING] No se pudieron eliminar algunos archivos temporales (en uso)")
        except Exception as e:
            logger.warning(f"[WARNING] Advertencia limpiando temporales: {e}")

    def _verificar_espacio_disco(self):
        """Verifica espacio libre en disco"""
        try:
            stat = shutil.disk_usage(self.carpeta_files)
            espacio_libre_gb = stat.free / (1024**3)
            porcentaje_libre = (stat.free / stat.total) * 100
            
            logger.info(f"[DISCO] Espacio libre: {espacio_libre_gb:.1f}GB ({porcentaje_libre:.1f}%)")
            
            if espacio_libre_gb < 1.0 or porcentaje_libre < 10:
                logger.warning(f"[WARNING] ADVERTENCIA: Poco espacio en disco ({espacio_libre_gb:.1f}GB libre)")
                
        except Exception as e:
            logger.warning(f"[WARNING] No se pudo verificar espacio en disco: {e}")

