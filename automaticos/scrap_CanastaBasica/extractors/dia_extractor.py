import os
import time
import re
import pickle
import unicodedata
import pandas as pd
import logging
from dotenv import load_dotenv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver import ActionChains
import pickle

load_dotenv()

# Configurar logging
#logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class DiaExtractor:
    """Extractor mejorado para Dia combinando lo mejor de ambos enfoques"""
    
    # Configuraciones centralizadas (estilo Delimart)
    CONFIG = {
        'timeout': 30,
        'wait_between_requests': 2,
        'supermarket_name': 'Dia',
        'base_url': 'https://diaonline.supermercadosdia.com.ar/'
    }
    
    def __init__(self):
        self.nombre_super = self.CONFIG['supermarket_name']
        self.timeout = self.CONFIG['timeout']
        self.driver = None
        self.wait = None
        self.sesion_iniciada = False
        self.cookies_file = "dia_cookies.pkl"
        self.email = 'manumarder@gmail.com'
        self.dni = "45374423"
        self.password = 'Ipecd2025'
        self.sucursal_calle = "Libertad"
    
    def setup_driver(self):
        """Configura el driver de Selenium"""
        if self.driver is None:
            options = Options()
            # Descomentar para producción
            # options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--ignore-ssl-errors')
            options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option('excludeSwitches', ['enable-automation'])
            options.add_experimental_option('useAutomationExtension', False)
            
            self.driver = webdriver.Chrome(options=options)
            self.wait = WebDriverWait(self.driver, self.timeout)
        
        return self.driver, self.wait
    
    # ==========================================
    # LÓGICA DE SESIÓN (LOGIN ASISTIDO)
    # ==========================================

    def asegurar_sesion_activa(self):
        if self.driver is None:
            self.setup_driver()
            
        # 1. Intentar cargar cookies previas
        if not self.sesion_iniciada and os.path.exists(self.cookies_file):
            try:
                self.driver.get(self.CONFIG['base_url'])
                cookies = pickle.load(open(self.cookies_file, "rb"))
                for cookie in cookies:
                    try:
                        self.driver.add_cookie(cookie)
                    except:
                        pass
                self.driver.refresh()
                time.sleep(5)
                
                if self._verificar_login_exitoso():
                    self.sesion_iniciada = True
                    logger.info(" [OK] Sesión recuperada desde cookies.")
                    return True
            except:
                pass

        # 2. LOGIN MANUAL ASISTIDO (Si fallaron las cookies)
        if not self.sesion_iniciada:
            if not self.login_solicitado:
                logger.info("---------------------------------------------------------")
                logger.info(" SE REQUIERE LOGIN MANUAL - PROCESO INICIADO")
                logger.info("---------------------------------------------------------")
                
                self.driver.get("https://diaonline.supermercadosdia.com.ar/")
                time.sleep(6) # Esperar a que cargue la home completamente
                
                # --- PASO CLAVE: ABRIR EL MODAL AUTOMÁTICAMENTE ---
                try:
                    logger.info(">> Intentando abrir menú 'Elegí tu envío'...")
                    
                    # Buscamos por ID 'region-locator-trigger' (el de tu HTML)
                    boton_ubicacion = self.wait.until(EC.presence_of_element_located((By.ID, "region-locator-trigger")))
                    
                    # Scroll para asegurar que sea visible
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", boton_ubicacion)
                    time.sleep(1)
                    
                    # Click JS forzado (ignora pointer-events y bloqueos)
                    self.driver.execute_script("arguments[0].click();", boton_ubicacion)
                    logger.info(">> Click enviado al botón de ubicación.")
                    
                except Exception as e:
                    logger.warning(f" No se pudo hacer click automático: {e}")
                    logger.warning(" POR FAVOR ABRE EL MENÚ MANUALMENTE.")

                # --- ESPERA ACTIVA PARA QUE EL USUARIO COMPLETE ---
                logger.info(">> TIENES 60 SEGUNDOS PARA LOGUEARTE Y ELEGIR SUCURSAL <<")
                
                tiempo_espera = 60
                for i in range(tiempo_espera):
                    if i % 10 == 0:
                        logger.info(f" Esperando login... {tiempo_espera - i}s restantes")
                    
                    if self._verificar_login_exitoso():
                        logger.info(" [OK] Login detectado exitosamente!")
                        self.sesion_iniciada = True
                        self.guardar_sesion()
                        return True
                    time.sleep(1)
                
                self.login_solicitado = True 
            
            # Último intento de verificación
            if self._verificar_login_exitoso():
                self.sesion_iniciada = True
                self.guardar_sesion()
                return True
            else:
                logger.error(" [ERROR] No se detectó el login tras la espera.")
                return False
        
        return True

    def _verificar_login_exitoso(self):
        """Verifica si aparece la dirección o el usuario logueado"""
        try:
            indicadores = [
                "//span[contains(text(), 'Retiras en')]",
                "//div[contains(text(), 'Retiras en')]",
                "//*[contains(text(), 'Hola')]",
                "//span[contains(@class, 'profile')]"
            ]
            for xpath in indicadores:
                if len(self.driver.find_elements(By.XPATH, xpath)) > 0:
                    return True
            return False
        except:
            return False

    def guardar_sesion(self):
        if self.driver:
            try:
                pickle.dump(self.driver.get_cookies(), open(self.cookies_file, "wb"))
            except: pass

    # ==========================================
    # EXTRACCIÓN DE PRODUCTOS
    # ==========================================

    def extract_products(self, urls):
        resultados = []
        if not self.asegurar_sesion_activa():
            return resultados
            
        for i, url in enumerate(urls, 1):
            logger.info(f"Procesando {i}/{len(urls)}")
            prod = self.extraer_producto(url)
            if prod:
                resultados.append(prod)
            time.sleep(self.CONFIG['wait_between_requests'])
        return resultados

    def extraer_producto(self, url):
        try:
            self.driver.get(url)
            time.sleep(4) # Esperar renderizado VTEX

            if "404" in self.driver.title:
                return None

            # 1. NOMBRE (Corregido con tu HTML)
            name = self._extract_name(url)
            if not name: 
                logger.warning(f" No pude extraer nombre de {url}")
                return None

            # 2. PRECIOS
            prices = self._extract_prices_fixed()

            # 3. CONSTRUCCIÓN DE DATOS (Sin columna Stock)
            data = {
                "supermercado": "Dia",
                "producto_nombre": name,
                "nombre": name,           # Clave extra por si tu CSV busca esta
                "name": name,
                "precio_descuento": prices['selling_price'], 
                "precio_normal": prices['list_price'],
                "precio_por_unidad": self._extract_unit_price(),
                "unidad_medida": "",
                "descuentos": str(self._extract_discounts()),
                # "stock" ELIMINADO SEGÚN PEDIDO
                "url": url,
                "fecha_extraccion": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            logger.info(f" Extraído: {name} | $ {data['precio_descuento']}")
            return data

        except Exception as e:
            logger.error(f" Error: {e}")
            return None

    def _extract_name(self, url_fallback):
        """Intenta extraer nombre de H1, Clase o URL como último recurso"""
        
        # 1. Intento por H1 (Tu HTML lo tiene así)
        try:
            h1 = self.driver.find_element(By.TAG_NAME, "h1")
            texto = h1.text.strip()
            if texto: return texto
        except: pass

        # 2. Intento por clase específica de marca/nombre
        try:
            elem = self.driver.find_element(By.CSS_SELECTOR, "span.vtex-store-components-3-x-productBrand")
            texto = elem.text.strip()
            if texto: return texto
        except: pass

        # 3. FALLBACK: URL (Si todo falla, sacamos el nombre del link)
        try:
            # De '.../aceite-cocinero-15-lt-25338/p' saca 'Aceite Cocinero 15 Lt'
            slug = url_fallback.split('.ar/')[-1].split('/p')[0]
            partes = slug.split('-')
            # Quitamos el ID final si es numérico
            if partes[-1].isdigit(): partes.pop()
            nombre_url = " ".join(partes).title()
            logger.warning(f" Usando nombre desde URL: {nombre_url}")
            return nombre_url
        except:
            return "Producto Desconocido"

    def _extract_prices_fixed(self):
        prices = {'list_price': 0.0, 'selling_price': 0.0}
        try:
            # Precio Venta
            selling_elem = self.driver.find_element(By.CSS_SELECTOR, "[class*='sellingPriceValue']")
            prices['selling_price'] = self._clean_price(selling_elem.text)
            
            # Precio Lista
            try:
                list_elem = self.driver.find_element(By.CSS_SELECTOR, "[class*='listPriceValue']")
                prices['list_price'] = self._clean_price(list_elem.text)
            except:
                prices['list_price'] = prices['selling_price']

            if prices['list_price'] == 0: prices['list_price'] = prices['selling_price']
        except:
            pass
        return prices

    def _clean_price(self, text):
        if not text: return 0.0
        try:
            return float(re.sub(r'[^\d,]', '', text).replace('.', '').replace(',', '.'))
        except:
            return 0.0

    def _extract_unit_price(self):
        try: return self.driver.find_element(By.CSS_SELECTOR, "[class*='measurementUnit']").text
        except: return ""

    def _extract_discounts(self):
        try:
            return [el.text for el in self.driver.find_elements(By.CSS_SELECTOR, ".vtex-product-highlights-2-x-productHighlightText") if el.text]
        except: return []

    def cleanup_driver(self):
        if self.driver:
            self.driver.quit()
            self.driver = None