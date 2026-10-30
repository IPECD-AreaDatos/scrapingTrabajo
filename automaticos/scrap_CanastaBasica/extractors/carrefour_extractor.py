import os
import sys
import time
import re
import unicodedata
import pandas as pd
import logging
from dotenv import load_dotenv
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Agregar directorio padre al path para imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.cookie_manager import CookieManager
from utils.optimization import optimize_driver_options, SmartWait, create_driver_with_retry
import random
from selenium_stealth import stealth

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()


class CarrefourExtractor:
    """Extractor específico para Carrefour con autenticación OAuth Mi Carrefour."""
    
    def __init__(self):
        self.nombre_super = "Carrefour"
        self.timeout = 15
        self.driver = None
        self.wait = None
        self.sesion_iniciada = False
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.files_dir = os.path.join(base_dir, 'files')
        os.makedirs(self.files_dir, exist_ok=True)
        
        # CookieManager centralizado
        self.cookie_manager = CookieManager(base_dir)
        self.cookies_file = str(self.cookie_manager.get_cookie_path('carrefour'))
        
        # Credenciales de Mi Carrefour (DNI sin puntos y contraseña)
        self.dni = "45374423"              
        self.password = "45374423Ctes"
        
        # Lista de User-Agents reales para rotación
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        ]
    
    def setup_driver(self, user_agent=None):
        """Configura el driver de Selenium con optimización y stealth."""
        if self.driver is None:
            options = Options()
            optimize_driver_options(options)
            options.page_load_strategy = 'eager'
            
            ua = user_agent if user_agent else random.choice(self.user_agents)
            logger.info(f"[DRIVER] Usando User-Agent: {ua}")
            options.add_argument(f'user-agent={ua}')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option('excludeSwitches', ['enable-automation'])
            options.add_experimental_option('useAutomationExtension', False)
            
            self.driver = create_driver_with_retry(options, max_retries=2, wait_seconds=5)
            
            if self.driver:
                self.wait = WebDriverWait(self.driver, self.timeout)
                stealth(
                    self.driver,
                    languages=["es-ES", "es"],
                    vendor="Google Inc.",
                    platform="Win32",
                    webgl_vendor="Intel Inc.",
                    renderer="Intel Iris OpenGL Engine",
                    fix_hairline=True,
                )
        
        return self.driver, self.wait

    def _cerrar_popups_molestos(self):
        """Cierra modales de ubicación, cookies y publicidad."""
        if not self.driver:
            return

        try:
            # 1. Cartel de Cookies
            selectores_cookies = [
                "//button[contains(text(), 'Aceptar todo')]",
                "//button[contains(text(), 'Rechazar todo')]",
                "//button[contains(text(), 'Aceptar')]",
                "//button[contains(text(), 'Entendido')]",
                "#onetrust-accept-btn-handler",
                ".vtex-cookie-consent-0-x-hideButton"
            ]
            for selector in selectores_cookies:
                try:
                    if selector.startswith("//"):
                        elementos = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elementos = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elementos and elementos[0].is_displayed():
                        self.driver.execute_script("arguments[0].click();", elementos[0])
                        logger.info(f"[POPUP] Cookie banner cerrado con: {selector}")
                        time.sleep(0.5)
                        break
                except Exception:
                    continue

            # 2. Modal de Sucursal / Ubicación
            botones_sucursal = self.driver.find_elements(
                By.XPATH, 
                "//button[contains(@class, 'confirm') or contains(text(), 'Confirmar') or contains(text(), 'Si, continuar')]"
            )
            if botones_sucursal:
                for btn in botones_sucursal:
                    if btn.is_displayed():
                        self.driver.execute_script("arguments[0].click();", btn)
                        logger.info("[POPUP] Modal de sucursal confirmado")
                        time.sleep(0.5)

            # 3. Publicidades y botones de cerrar
            selectores_cerrar = [
                "button[aria-label='Close']",
                ".vtex-modal-layout-0-x-closeButton",
                ".c-close",
                ".close-button",
                "//button[contains(text(), 'No, gracias')]"
            ]
            for sel in selectores_cerrar:
                try:
                    if sel.startswith("//"):
                        elementos = self.driver.find_elements(By.XPATH, sel)
                    else:
                        elementos = self.driver.find_elements(By.CSS_SELECTOR, sel)
                    for btn in elementos:
                        if btn.is_displayed():
                            self.driver.execute_script("arguments[0].click();", btn)
                            logger.info(f"[POPUP] Popup cerrado con: {sel}")
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"Error silencioso en _cerrar_popups_molestos: {e}")

    def extraer_producto(self, url: str) -> dict:
        """Extrae los datos de un producto individual."""
        try:
            logger.info(f"[EXTRACT] Navegando al producto: {url}")

            if not isinstance(url, str) or not url.strip().startswith(('http://', 'https://')):
                logger.warning(f"[EXTRACT] URL inválida: {url!r}")
                return {"error_type": "invalid_url", "url": url, "titulo": "URL inválida"}

            if not self.sesion_iniciada:
                if not self.asegurar_sesion_activa():
                    logger.error("[ERROR] No se pudo establecer sesión ni cargar cookies")
                    return {"error_type": "no_session", "url": url, "titulo": "No se pudo establecer sesión"}

            self.driver.get(url)
            SmartWait.wait_minimal(3.0)
            self._cerrar_popups_molestos()

            if self._es_pagina_error():
                logger.warning(f"[WARNING] Página no encontrada (404): {url}")
                return {"error_type": "404", "url": url, "titulo": self.driver.title}

            if self._producto_no_disponible():
                logger.warning("[STOCK] Producto sin stock.")
                return {"error_type": "no_disponible", "url": url, "titulo": self.driver.title}

            nombre = self._extraer_nombre(self.wait)
            if not nombre:
                return {"error_type": "no_name", "url": url, "titulo": self.driver.title}

            precio_desc = self._extraer_precio_descuento(self.driver)
            precio_normal = self._extraer_precio_normal(self.driver, precio_desc)

            if precio_desc == "0" and precio_normal == "0":
                logger.warning(f"[EXTRACT] No se encontraron precios para: {nombre}. Asumiendo sin stock.")
                return {"error_type": "no_disponible", "url": url, "nombre": nombre, "titulo": self.driver.title}

            precio_completo, unidad_text = self._extraer_precio_unidad(self.driver)
            descuentos = self._extraer_descuentos(self.driver)

            resultado = {
                "nombre": nombre,
                "precio_normal": self._clean_price(precio_normal),
                "precio_descuento": self._clean_price(precio_desc),
                "precio_por_unidad": self._clean_price(precio_completo),
                "unidad": unidad_text,
                "descuentos": " | ".join(descuentos) if descuentos else "Ninguno",
                "fecha": datetime.today().strftime("%Y-%m-%d"),
                "supermercado": self.nombre_super,
                "url": url
            }
            return resultado

        except Exception as e:
            logger.error(f"[EXTRACT] Error crítico: {e}")
            if any(term in str(e).lower() for term in ["timeout", "renderer", "disconnected"]):
                self.cleanup_driver()
            return {"error_type": "exception", "url": url, "titulo": str(e)}

    def extract_products(self, urls: List[str]) -> List[dict]:
        """Extrae múltiples productos manteniendo la misma sesión."""
        resultados = []
        if not self.asegurar_sesion_activa():
            logger.error("No se pudo establecer sesión para la extracción")
            return resultados

        for i, url in enumerate(urls, 1):
            logger.info(f"Extrayendo producto {i}/{len(urls)}")
            producto = self.extraer_producto(url)
            if producto:
                resultados.append(producto)
            SmartWait.wait_minimal(0.2)

        self.guardar_sesion()
        return resultados

    def _extraer_nombre(self, wait):
        selectores = [
            "h1 .vtex-store-components-3-x-productBrand",
            "h1 span.vtex-store-components-3-x-productBrand",
            "h1.vtex-store-components-3-x-productNameContainer",
            "h1[data-testid='product-name']",
            "h1"
        ]
        for intento in range(2):
            for selector in selectores:
                try:
                    elemento = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                    nombre = elemento.text.strip()
                    if not nombre:
                        nombre = elemento.get_attribute("textContent").strip()
                    if nombre:
                        return nombre
                except Exception:
                    continue
            if intento == 0:
                time.sleep(1.5)
        return None

    def _extraer_precio_descuento(self, driver):
        selectores = [
            ".vtex-product-price-1-x-sellingPriceValue",
            "span.valtech-carrefourar-product-price-0-x-sellingPriceValue",
            "span.valtech-carrefourar-product-price-0-x-currencyInteger",
            "span.valtech-carrefourar-product-price-0-x-currencyContainer",
            "span[class*='sellingPrice']",
            "[class*='sellingPriceValue']",
            "[data-testid='price-value']"
        ]
        return self._buscar_precio(driver, selectores, "0")

    def _extraer_precio_normal(self, driver, precio_desc):
        selectores = [
            ".vtex-product-price-1-x-listPriceValue",
            "span.valtech-carrefourar-product-price-0-x-listPriceValue",
            "span[class*='listPrice']",
            "[class*='listPriceValue']",
            ".list-price",
            ".original-price"
        ]
        precio = self._buscar_precio(driver, selectores, precio_desc)
        return precio if precio != precio_desc else precio_desc

    def _extraer_precio_unidad(self, driver):
        precio_completo = ""
        unidad_text = ""
        try:
            contenedor_unidad = driver.find_element(By.CSS_SELECTOR, "div.valtech-carrefourar-dynamic-weight-price-0-x-container")
            try:
                precio_elem = contenedor_unidad.find_element(By.CSS_SELECTOR, "span.valtech-carrefourar-dynamic-weight-price-0-x-currencyContainer")
                precio_completo = precio_elem.text.strip()
            except Exception:
                pass
            try:
                unidad_elem = contenedor_unidad.find_element(By.CSS_SELECTOR, "span.valtech-carrefourar-dynamic-weight-price-0-x-unit")
                unidad_text = unidad_elem.text.strip()
            except Exception:
                pass
        except Exception:
            try:
                precio_unidad = driver.find_element(By.XPATH, "//*[contains(text(), 'kg') or contains(text(), 'l') or contains(text(), 'unidad')]")
                texto = precio_unidad.text.strip()
                if texto:
                    unidad_text = texto
            except Exception:
                pass
        return precio_completo, unidad_text

    def _extraer_descuentos(self, driver):
        descuentos = []
        try:
            selectores_descuentos = [
                ".valtech-carrefourar-product-price-0-x-discountContainer",
                ".promo-badge",
                ".promotion-label",
                ".discount-badge",
                ".offer-tag",
                ".savings",
                "[class*='discount']",
                "[class*='promo']",
                "[class*='offer']",
                "[class*='saving']",
                ".vtex-product-price-1-x-saving",
                ".vtex-store-components-3-x-discountContainer",
                ".carrefourar-product-price-0-x-discount",
                ".vecs-product-price-0-x-discount"
            ]
            for selector in selectores_descuentos:
                try:
                    elementos = driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elementos:
                        if elem.is_displayed():
                            texto = elem.text.strip()
                            if self.descuento_es_valido(texto):
                                descuentos.append(texto)
                except Exception:
                    continue

            palabras_clave = [
                "OFF", "OFF%", "%", "DESCUENTO", "DCTO", "SAVE", "AHORRO", 
                "PROMO", "OFERTA", "2x1", "3x2", "Llevá", "Lleve", "Bonific"
            ]
            for palabra in palabras_clave:
                try:
                    elementos = driver.find_elements(
                        By.XPATH, 
                        f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{palabra.lower()}')]"
                    )
                    for elem in elementos:
                        if elem.is_displayed():
                            texto = elem.text.strip()
                            if self.descuento_es_valido(texto):
                                descuentos.append(texto)
                except Exception:
                    continue
        except Exception as e:
            logger.error(f"Error extrayendo descuentos: {str(e)}")

        return self.procesar_descuentos(descuentos)

    def limpiar_descuento(self, texto: str) -> str:
        if not texto:
            return ""
        texto = texto.encode('ascii', 'ignore').decode('ascii').lower()
        texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
        texto = " ".join(texto.split())
        texto = re.sub(r"[^a-z0-9% x/\.]", "", texto)
        texto = texto.replace(" %", "%").replace("% ", "% ")
        return texto.strip()

    def descuento_es_valido(self, texto: str) -> bool:
        if not texto or len(texto) > 80:
            return False
        text = texto.lower()
        keywords = ["%", "off", "descuento", "dcto", "promo", "oferta", "2x1", "3x2"]
        return any(k in text for k in keywords) and any(c.isdigit() for c in text)

    def normalizar_descuento(self, texto: str) -> str:
        t = re.sub(r"(\d+)\s*%", r"\1%", texto)
        t = t.replace("off", " off")
        return " ".join(t.split()).strip()

    def procesar_descuentos(self, descuentos_raw):
        final = []
        for d in descuentos_raw:
            limpio = self.limpiar_descuento(d)
            if self.descuento_es_valido(limpio):
                final.append(self.normalizar_descuento(limpio))
        final = list(dict.fromkeys(final))
        final.sort(key=lambda x: "%" not in x)
        return final

    def _buscar_precio(self, driver, selectores, default):
        """Busca precio con reintentos para asegurar renderizado en VTEX."""
        for intento in range(3):
            for selector in selectores:
                try:
                    elementos = driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elementos:
                        texto = elem.text.strip()
                        if not texto:
                            texto = elem.get_attribute("textContent").strip()
                        if texto and any(c.isdigit() for c in texto):
                            return texto
                except Exception:
                    continue
            if intento < 2:
                time.sleep(2)
        return default

    def login_con_email_password(self):
        """Inicia sesión siguiendo el flujo de Mi Carrefour y popup OAuth."""
        try:
            logger.info("=== LOGIN MI CARREFOUR ===")
            self.driver.get("https://www.carrefour.com.ar/")
            SmartWait.wait_minimal(2)
            self._cerrar_popups_molestos()

            # 1. Abrir menú de usuario (personita) y clickear "Ingresar con Mi Carrefour"
            if not self.hacer_clic_ingresar_con_mail():
                return False

            # 2. Completar DNI y contraseña en la ventana OAuth
            if not self.ingresar_credenciales_con_debug():
                return False

            # 3. Volver al foco de la ventana principal
            origin_handle = getattr(self, "_login_origin_handle", None)
            if origin_handle and origin_handle in self.driver.window_handles:
                self.driver.switch_to.window(origin_handle)
            else:
                self.driver.switch_to.window(self.driver.window_handles[0])

            SmartWait.wait_minimal(4)
            if self.verificar_sesion_con_debug():
                self.sesion_iniciada = True
                self.guardar_sesion()
                logger.info("[OK] LOGIN MI CARREFOUR EXITOSO")
                return True

            logger.error("Login enviado, pero no se pudo confirmar la sesión activa")
            return False
        except Exception as e:
            logger.error(f"Error en login: {e}")
            return False

    def hacer_clic_ingresar_con_mail(self):
        """Abre el modal de cuenta y hace clic en 'Ingresar con Mi Carrefour'."""
        try:
            self._cerrar_popups_molestos()

            selectores_persona = [
                "div[class*='myAccountWrapper'] div[role='button']",
                "div[class*='myAccountWrapperLink']",
                "[data-testid='user-menu']",
                "//div[contains(@class, 'myAccountWrapper')]//div[@role='button']"
            ]
            btn_persona = None
            for sel in selectores_persona:
                try:
                    if sel.startswith("//"):
                        elems = self.driver.find_elements(By.XPATH, sel)
                    else:
                        elems = self.driver.find_elements(By.CSS_SELECTOR, sel)
                    if elems and elems[0].is_displayed():
                        btn_persona = elems[0]
                        break
                except Exception:
                    continue

            if not btn_persona:
                self.driver.get("https://www.carrefour.com.ar/login")
                SmartWait.wait_minimal(2)
                self._cerrar_popups_molestos()
            else:
                self.driver.execute_script("arguments[0].click();", btn_persona)
                SmartWait.wait_minimal(1.5)

            self._login_origin_handle = self.driver.current_window_handle
            handles_previos = set(self.driver.window_handles)

            selectores_btn_carrefour = [
                ".vtex-login-2-x-customOAuthOptionBtn button",
                "div.vtex-login-2-x-buttonSocial button",
                "//button[contains(., 'Ingresar con Mi Carrefour')]",
                "//button[contains(., 'Mi Carrefour')]",
                "//div[contains(@class, 'vtex-button__label') and contains(., 'Mi Carrefour')]/parent::button"
            ]
            btn_oauth = None
            for sel in selectores_btn_carrefour:
                try:
                    if sel.startswith("//"):
                        elem = self.wait.until(EC.element_to_be_clickable((By.XPATH, sel)))
                    else:
                        elem = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
                    if elem:
                        btn_oauth = elem
                        break
                except Exception:
                    continue

            if not btn_oauth:
                logger.error("No se pudo encontrar el botón 'Ingresar con Mi Carrefour'")
                return False

            self.driver.execute_script("arguments[0].click();", btn_oauth)
            WebDriverWait(self.driver, 10).until(
                lambda d: len(d.window_handles) > len(handles_previos) or "id.carrefour.com.ar" in d.current_url
            )

            nuevos_handles = [h for h in self.driver.window_handles if h not in handles_previos]
            if nuevos_handles:
                self.driver.switch_to.window(nuevos_handles[-1])

            WebDriverWait(self.driver, 15).until(EC.presence_of_element_located((By.ID, "username")))
            return True
        except Exception as e:
            logger.error(f"Fallo al abrir ventana de Mi Carrefour: {e}")
            return False

    def ingresar_credenciales_con_debug(self):
        """Ingresa DNI y contraseña en Keycloak (id.carrefour.com.ar)."""
        try:
            if not self.dni or not self.password:
                logger.error("Faltan CARREFOUR_DNI o CARREFOUR_PASSWORD en el archivo .env")
                return False

            campo_dni = self.wait.until(EC.element_to_be_clickable((By.ID, "username")))
            campo_dni.clear()
            campo_dni.send_keys(self.dni)
            SmartWait.wait_minimal(0.3)

            campo_pwd = self.wait.until(EC.element_to_be_clickable((By.ID, "password")))
            campo_pwd.clear()
            campo_pwd.send_keys(self.password)
            SmartWait.wait_minimal(0.3)

            selectores_submit = [
                "#kc-login",
                "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'iniciar sesión')]",
                "//button[@type='submit']",
                "//input[@type='submit']"
            ]
            btn_submit = None
            for sel in selectores_submit:
                try:
                    if sel.startswith("//"):
                        btn_submit = self.driver.find_element(By.XPATH, sel)
                    else:
                        btn_submit = self.driver.find_element(By.CSS_SELECTOR, sel)
                    if btn_submit.is_displayed():
                        break
                except Exception:
                    continue

            if not btn_submit:
                campo_pwd.send_keys(Keys.RETURN)
            else:
                self.driver.execute_script("arguments[0].click();", btn_submit)

            logger.info("Credenciales enviadas correctamente")
            return True
        except Exception as e:
            logger.error(f"Error cargando credenciales: {e}")
            return False

    def verificar_sesion_con_debug(self):
        try:
            SmartWait.wait_minimal(1)
            current_url = self.driver.current_url
            if 'login' in current_url.lower():
                return False

            indicadores = [
                "//*[contains(text(), 'Mi cuenta')]",
                "//*[contains(text(), 'Hola')]",
                "//*[contains(text(), 'Cuenta')]",
                ".my-account",
                "[data-testid='user-menu']"
            ]
            for i in range(3):
                for indicador in indicadores:
                    try:
                        if indicador.startswith("//"):
                            elemento = self.driver.find_element(By.XPATH, indicador)
                        else:
                            elemento = self.driver.find_element(By.CSS_SELECTOR, indicador)
                        if elemento.is_displayed():
                            logger.info(f"Sesión activa confirmada: {indicador}")
                            return True
                    except Exception:
                        continue
                time.sleep(3)
            return False
        except Exception as e:
            logger.error(f"Error verificando sesión: {e}")
            return False

    def asegurar_sesion_activa(self):
        if self.driver is None:
            self.setup_driver()

        for intento in range(2):
            if intento > 0:
                self.cleanup_driver()
                self.setup_driver(user_agent=random.choice(self.user_agents))

            if intento == 0:
                cookies = self.cookie_manager.load_cookies('carrefour')
                if cookies:
                    try:
                        self.driver.get("https://www.carrefour.com.ar")
                        self.driver.delete_all_cookies()
                        for cookie in cookies:
                            try:
                                self.driver.add_cookie(cookie)
                            except Exception:
                                pass
                        self.driver.refresh()
                        SmartWait.wait_minimal(1.5)
                        if self.verificar_sesion_con_debug():
                            self.sesion_iniciada = True
                            logger.info("[OK] Sesión cargada desde cookies")
                            return True
                    except Exception as e:
                        logger.debug(f"Error cargando sesión: {e}")

            if self.login_con_email_password():
                self.sesion_iniciada = True
                return True

            if intento < 1:
                SmartWait.wait_minimal(2)

        return False

    def guardar_sesion(self):
        try:
            if self.driver and self.sesion_iniciada:
                cookies = self.driver.get_cookies()
                return self.cookie_manager.save_cookies('carrefour', cookies)
            return False
        except Exception as e:
            logger.error(f"Error guardando sesión: {e}")
            return False

    def _es_pagina_error(self):
        try:
            indicadores = [
                "¡ups!", "página no encontrada", "página no existe", 
                "404", "no encontrada", "¡disculpanos!"
            ]
            titulo = self.driver.title.lower()
            if any(ind in titulo for ind in indicadores):
                return True
            if "/p" in self.driver.current_url.lower() and "las mejores ofertas en supermercados" in titulo:
                return True
            return False
        except Exception:
            return False

    def _producto_no_disponible(self):
        try:
            selectores = [
                "//div[contains(@class, 'product-info')]//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'no disponible')]",
                "//div[contains(@class, 'product-info')]//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'sin stock')]",
                "//*[contains(@class, 'buy-button-out-of-stock')]"
            ]
            for sel in selectores:
                for elem in self.driver.find_elements(By.XPATH, sel):
                    if elem.is_displayed():
                        return True
            return False
        except Exception:
            return False

    def _clean_price(self, price_text):
        if not price_text or price_text == "0":
            return "0"
        try:
            txt = str(price_text).strip()
            clean_price = re.sub(r'[^\d,.]', '', txt)
            # Quitar puntos de miles y reemplazar coma por punto
            clean_price = clean_price.replace('.', '').replace(',', '.')
            price_float = float(clean_price)
            return str(int(price_float)) if price_float.is_integer() else f"{price_float:.2f}"
        except Exception:
            return "0"

    def cleanup_driver(self):
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
                self.wait = None
                self.sesion_iniciada = False
                logger.info("Driver cerrado correctamente")
        except Exception as e:
            logger.error(f"Error cerrando driver: {e}")