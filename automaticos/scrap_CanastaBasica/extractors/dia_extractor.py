import os
import time
import re
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
from selenium.webdriver.common.keys import Keys
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
        self.dni = 45374423
        self.password = 'Ipecd2025'
    
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
    
    def extract_products(self, urls):
        """Extrae múltiples productos manteniendo la misma sesión para DIA"""
        resultados = []
        
        # Validación inicial de sesión
        if not self.asegurar_sesion_activa():
            logger.error("No se pudo establecer sesión para la extracción en Dia")
            return resultados
        
        for i, url in enumerate(urls, 1):
            logger.info(f"Extrayendo producto {i}/{len(urls)}")
            logger.info(f"URL: {url}")

            producto = self.extraer_producto(url)
            if producto:
                resultados.append(producto)
            
            # Pausa para no saturar al servidor de Dia
            time.sleep(self.CONFIG['wait_between_requests'])
        
        self.guardar_sesion()
        return resultados

    def extraer_producto(self, url):
        """Extrae datos de un producto individual en DIA (VTEX)"""
        try:
            # Asegurar sesión activa si se cayó
            if not self.sesion_iniciada:
                if not self.asegurar_sesion_activa():
                    logger.error("No se pudo recuperar la sesión en Dia")
                    return None
            
            logger.info(f"[SEARCH] Navegando a: {url}")
            self.driver.get(url)
            
            # ═══════════════════════════════════════════════════════════
            # ESPERAS MEJORADAS - ADAPTADAS A VTEX (DIA)
            # ═══════════════════════════════════════════════════════════
            
            logger.info("Esperando carga completa de la página...")
            time.sleep(3)  # Dia suele ser lento cargando componentes reactivos
            
            # Espera 1: Título del producto (nombre)
            try:
                # En VTEX el título suele ser vtex-store-components-3-x-productNameContainer
                self.wait.until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
                logger.info("[OK] Título h1 cargado")
            except Exception as e:
                logger.warning(f"[WARNING] Timeout esperando h1: {e}")
            
            # Espera 2: Contenedor de PRECIO (Crítico en VTEX)
            logger.info("Esperando elementos de precio VTEX...")
            try:
                # Selectores típicos de precio en Dia/VTEX
                selectores_precio_espera = [
                    ".vtex-product-price-1-x-sellingPriceValue",
                    ".vtex-store-components-3-x-sellingPrice",
                    ".diaio-store-theme-1-x-sellingPriceValue" 
                ]
                
                precio_cargado = False
                for selector in selectores_precio_espera:
                    try:
                        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                        logger.info(f"[OK] Contenedor de precio cargado: {selector}")
                        precio_cargado = True
                        break
                    except:
                        continue
                
                if not precio_cargado:
                    logger.warning("[WARNING] No apareció el precio principal en el tiempo esperado")
                
                # Espera adicional para renderizado final de descuentos
                time.sleep(2)
                
            except Exception as e:
                logger.warning(f"[WARNING] Error esperando precio: {e}")
            
            # ═══════════════════════════════════════════════════════════
            # PASO 1: Verificar si es página de error
            # ═══════════════════════════════════════════════════════════
            # Dia suele redirigir al home o mostrar un 404 genérico
            if "404" in self.driver.title or "Página no encontrada" in self.driver.title:
                logger.warning(f"[ERROR] Página no encontrada (404): {url}")
                return {"error_type": "404", "url": url, "titulo": self.driver.title}
            
            # ═══════════════════════════════════════════════════════════
            # PASO 2: Verificar disponibilidad (Stock)
            # ═══════════════════════════════════════════════════════════
            # Importante: Necesitas adaptar _verificar_disponibilidad_detallada 
            # para buscar clases como '.vtex-availability-notify' o mensajes 'Sin stock'
            disponibilidad = self._verificar_disponibilidad_detallada()
            
            if disponibilidad["estado"] == "no_disponible":
                logger.warning(f"[STOP] PRODUCTO NO DISPONIBLE: {self.driver.title}")
                name = self._extract_name() # Intentamos sacar el nombre al menos
                return self._build_product_data(
                    name or "Producto sin nombre", 0, 0, None, None, ["NO DISPONIBLE"], url
                )
            
            # ═══════════════════════════════════════════════════════════
            # PASO 3: Extraer datos del producto
            # ═══════════════════════════════════════════════════════════
            
            name = self._extract_name()
            if not name:
                # Intento fallback por si el H1 falla
                try:
                    name = self.driver.find_element(By.CSS_SELECTOR, ".vtex-store-components-3-x-productNameContainer").text
                except:
                    pass

            if not name:
                logger.warning(f"[ERROR] No se pudo extraer nombre de {url}")
                return {"error_type": "no_name", "url": url, "titulo": self.driver.title}
            
            logger.info(f"[PRODUCT] Nombre extraído: {name}")
            
            # Extraer precios
            prices = self._extract_prices()
            unit_price, unit_text = self._extract_unit_price()
            discounts = self._extract_discounts()

            # Lógica especial para Dia: A veces el precio normal no aparece si hay oferta,
            # y el precio de lista está en otro selector tachado.
            if prices["normal"] == 0 and prices["descuento"] > 0:
                 prices["normal"] = prices["descuento"]

            product_data = self._build_product_data(
                name,
                prices["normal"],
                prices["descuento"],
                unit_price,
                unit_text,
                discounts,
                url
            )

            # ═══════════════════════════════════════════════════════════
            # PASO 4: Debug Final
            # ═══════════════════════════════════════════════════════════
            final_price = prices["descuento"] or prices["normal"]
            if final_price and final_price > 0:
                logger.info(f"[OK] EXTRACCIÓN EXITOSA: {name} - Precio: ${final_price}")
            else:
                logger.warning(f"[WARNING] Producto extraído pero sin precio válido: {name}")

            return product_data

        except Exception as e:
            logger.error(f"ERROR crítico extrayendo {url}: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            # No matamos la sesión inmediatamente por un error de producto, 
            # a menos que sea recurrente
            return None
    






    def login_con_email_password(self):
        """Login completo con DEBUGGING DETALLADO para DIA - Adaptado de Masonline"""
        try:
            logger.info("=== DEBUG LOGIN DIA ===")
            
            # Paso 1: Ir a página de login
            # En VTEX, /login suele redirigir al popup de autenticación
            logger.info("[SEARCH] Navegando a login de Dia...")
            self.driver.get("https://diaonline.supermercadosdia.com.ar/login")
            time.sleep(5)  # Dia es pesado cargando scripts, damos tiempo extra
            
            # DEBUG: Mostrar estructura básica para ver si cargó el modal
            logger.info("[SEARCH] Analizando si apareció el modal de login...")
            try:
                # Buscamos textos clave que indiquen que el login está visible
                body_text = self.driver.find_element(By.TAG_NAME, "body").text[:500]
                logger.info(f"Texto inicial del body: {body_text}")
            except:
                pass
            
            # Paso 2: Ingresar credenciales
            logger.info("[SEARCH] Intentando ingresar credenciales...")
            if not self.ingresar_credenciales_con_debug():
                return False
            
            # Paso 3: Verificar login
            logger.info("[SEARCH] Verificando login...")
            # Damos tiempo a que redirija al home o al perfil
            time.sleep(5)
            if self._verificar_sesion_activa():
                self.sesion_iniciada = True
                self.guardar_sesion()
                logger.info("[OK] LOGIN DIA EXITOSO")
                return True
            else:
                logger.error("[ERROR] LOGIN DIA FALLIDO")
                return False
                
        except Exception as e:
            logger.error(f"[ERROR] Error general en login Dia: {e}")
            return False

    def ingresar_credenciales_con_debug(self):
        """Ingresar credenciales en Dia con debugging"""
        try:
            logger.info("Procesando formulario de credenciales Dia...")
            time.sleep(2)
            
            # --- FASE 1: CAMBIAR A MODO CONTRASEÑA ---
            # Dia suele mostrar "Entrar con código de acceso" por defecto.
            # Buscamos el botón para cambiar a contraseña.
            logger.info("[SEARCH] Buscando botón 'Entrar con e-mail y contraseña'...")
            
            opciones_login = [
                "//button[contains(., 'Entrar con e-mail y contraseña')]",
                "//span[contains(text(), 'Entrar con e-mail y contraseña')]",
                "//div[contains(text(), 'Entrar con e-mail y contraseña')]",
                "//*[@class='vtex-login-2-x-emailPasswordOptionBtn']" # Clase común en VTEX
            ]
            
            opcion_encontrada = False
            for opcion in opciones_login:
                try:
                    elemento = self.driver.find_element(By.XPATH, opcion)
                    if elemento.is_displayed():
                        logger.info(f"[OK] Opción encontrada: {opcion}")
                        elemento.click()
                        opcion_encontrada = True
                        time.sleep(2) # Esperar a que cambien los inputs
                        break
                except Exception as e:
                    pass # Sigue buscando
            
            if not opcion_encontrada:
                logger.warning("[WARNING] No se encontró botón de cambio a password. Quizás ya estamos en el formulario correcto.")

            # --- FASE 2: CAMPO EMAIL ---
            logger.info("[SEARCH] Buscando campo Email...")
            campo_email = None
            selectores_email = [
                "input[name='email']", # Estándar VTEX
                "input[type='email']",
                "input[placeholder*='ejemplo@mail.com']",
                ".vtex-login-2-x-inputContainerEmail input"
            ]
            
            for selector in selectores_email:
                try:
                    campo_email = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                    if campo_email:
                        logger.info(f"[OK] Campo email encontrado: {selector}")
                        break
                except:
                    continue
            
            if not campo_email:
                logger.error("[ERROR] No se encontró campo Email.")
                return False
            
            campo_email.clear()
            campo_email.send_keys(self.email)
            logger.info("[OK] Email enviado")
            time.sleep(1)

            # --- FASE 3: CAMPO PASSWORD ---
            logger.info("[SEARCH] Buscando campo Password...")
            campo_password = None
            selectores_password = [
                "input[type='password']",
                "input[name='password']", # Estándar VTEX
                ".vtex-login-2-x-inputContainerPassword input"
            ]
            
            for selector in selectores_password:
                try:
                    campo_password = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if campo_password.is_displayed():
                        logger.info(f"[OK] Campo password encontrado: {selector}")
                        break
                except:
                    continue
            
            if not campo_password:
                logger.error("[ERROR] No se encontró campo Password (¿Quizás no se hizo click en 'Entrar con contraseña'?)")
                return False

            campo_password.clear()
            campo_password.send_keys(self.password)
            logger.info("[OK] Password enviado")
            time.sleep(1)

            # --- FASE 4: BOTÓN SUBMIT ---
            logger.info("[SEARCH] Buscando botón Entrar...")
            boton_login = None
            selectores_boton = [
                "//button[@type='submit']//div[contains(text(), 'Entrar')]",
                "//button[contains(., 'Entrar')]",
                "//div[contains(@class, 'vtex-login')]//button[@type='submit']"
            ]
            
            for selector in selectores_boton:
                try:
                    boton_login = self.driver.find_element(By.XPATH, selector)
                    if boton_login.is_displayed():
                        logger.info(f"[OK] Botón entrar encontrado: {selector}")
                        break
                except:
                    continue
            
            if not boton_login:
                logger.error("[ERROR] No se encontró botón submit.")
                return False
            
            try:
                boton_login.click()
                logger.info("[OK] Click en Entrar realizado")
            except:
                self.driver.execute_script("arguments[0].click();", boton_login)
                logger.info("[OK] Click JS forzado realizado")
            
            time.sleep(5) # Esperar validación del servidor
            return True

        except Exception as e:
            logger.error(f"[ERROR] Excepción en credenciales Dia: {e}")
            return False

    def _verificar_sesion_activa(self):
        """Verifica sesión en Dia buscando elementos de usuario logueado"""
        try:
            # Opción 1: Verificar URL (si no estamos en login)
            current_url = self.driver.current_url
            if "/login" not in current_url and "my-account" in current_url:
                return True

            # Opción 2: Buscar el saludo "Hola" o icono de perfil
            indicadores = [
                "//*[contains(text(), 'Hola')]", # Común en VTEX: "Hola, Juan"
                "//*[contains(@class, 'vtex-login-2-x-profile')]", # Icono de perfil logueado
                "//span[contains(text(), 'Mi cuenta')]",
                "//button[contains(text(), 'Cerrar sesión')]"
            ]
            
            for xpath in indicadores:
                try:
                    elements = self.driver.find_elements(By.XPATH, xpath)
                    if elements and any(e.is_displayed() for e in elements):
                        logger.info(f"[OK] Sesión verificada con elemento: {xpath}")
                        return True
                except:
                    continue
            
            return False
        except Exception as e:
            logger.warning(f"Error verificando sesión: {e}")
            return False

    def asegurar_sesion_activa(self):
        """Lógica principal para garantizar sesión Y UBICACIÓN"""
        if self.driver is None:
            self.setup_driver()
            
        # Intentamos ir al home 
        self.driver.get("https://diaonline.supermercadosdia.com.ar/")
        time.sleep(3)
        
        sesion_ok = False
        
        # Paso A: Verificar Login
        if self._verificar_sesion_activa():
            logger.info("Sesión recuperada por cookies/cache exitosamente.")
            sesion_ok = True
        else:
            logger.info("Sesión no detectada. Iniciando login fresco...")
            sesion_ok = self.login_con_email_password()
            
        # Paso B: Si el login está ok (o aunque fallara, intentamos), forzar ubicación
        if sesion_ok:
            self._configurar_ubicacion_corrientes() # <--- AQUÍ ESTÁ LA MAGIA
            
        return sesion_ok
    
    def _configurar_ubicacion_corrientes(self):
        """
        Flujo FINAL optimizado con el HTML proporcionado.
        1. Click en #region-locator-trigger (Botón 'Cambiar dirección' del header).
        2. Click en Confirmar (Dirección guardada).
        3. Click en Confirmar (Sucursal/Envío).
        """
        try:
            logger.info("[GEO] Iniciando configuración de ubicación (Modo Corrientes)...")
            time.sleep(3) # Esperar que el header se renderice completamente

            # --- PASO 1: ABRIR EL MODAL (Usando tu HTML exacto) ---
            logger.info("[GEO] Buscando botón de ubicación en el header...")
            boton_geo = None
            
            # Lista de selectores basada en tu HTML, de más específico a más general
            selectores_apertura = [
                (By.ID, "region-locator-trigger"),  # EL MEJOR: ID directo del HTML que pasaste
                (By.XPATH, "//input[@value='Cambiar dirección']"), # El botón interno
                (By.CSS_SELECTOR, ".diaio-region-locator-v2-0-x-sc__container"), # La clase del contenedor
                (By.XPATH, "//*[contains(text(), 'Retiras en')]") # Texto visible
            ]

            opened = False
            for tipo, selector in selectores_apertura:
                try:
                    boton_geo = self.driver.find_element(tipo, selector)
                    if boton_geo.is_displayed():
                        # A veces el click normal falla en headers flotantes, usamos JS por seguridad
                        self.driver.execute_script("arguments[0].click();", boton_geo)
                        logger.info(f"[GEO] Click en botón de ubicación exitoso ({selector})")
                        opened = True
                        break
                except Exception as e:
                    continue
            
            if not opened:
                logger.warning("[GEO] No se pudo hacer click en el botón de ubicación.")

            time.sleep(2) # Esperar animación del modal

            # --- PASO 2: PRIMER CONFIRMAR (Dirección guardada) ---
            logger.info("[GEO] Confirmando dirección guardada...")
            try:
                # Buscamos el botón rojo "Confirmar"
                boton_conf_1 = self.wait.until(EC.element_to_be_clickable(
                    (By.XPATH, "//button[contains(text(), 'Confirmar')]")
                ))
                boton_conf_1.click()
                logger.info("[GEO] Dirección confirmada.")
            except Exception as e:
                logger.warning(f"[GEO] Falló primer confirmar (¿Ya estaba seleccionado?): {e}")

            time.sleep(2) # Breve pausa entre modales

            # --- PASO 3: SEGUNDO CONFIRMAR (Sucursal Av. Libertad) ---
            logger.info("[GEO] Confirmando sucursal/envío...")
            try:
                # Buscamos el último botón confirmar que aparezca en el DOM (el del segundo modal)
                boton_conf_2 = self.wait.until(EC.element_to_be_clickable(
                    (By.XPATH, "(//button[contains(text(), 'Confirmar')])[last()]")
                ))
                boton_conf_2.click()
                logger.info("[GEO] Sucursal confirmada.")
            except Exception as e:
                logger.warning(f"[GEO] Falló segundo confirmar: {e}")

            # --- PASO 4: ESPERAR RECARGA ---
            logger.info("[GEO] Esperando actualización de precios...")
            time.sleep(4) 
            return True

        except Exception as e:
            logger.error(f"[GEO] Error crítico configurando ubicación: {e}")
            return False