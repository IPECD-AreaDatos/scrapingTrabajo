import os
import time
import pandas as pd
import logging
from dotenv import load_dotenv
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pickle

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

class ExtractorCarrefour:
    """Extractor específico para Carrefour"""
    
    def __init__(self):
        self.nombre_super = "Carrefour"
        self.timeout = 30  # Aumentar timeout para mejor estabilidad
        self.driver = None
        self.wait = None
        self.sesion_iniciada = False
        self.cookies_file = "carrefour_cookies.pkl"
        self.email = os.getenv('CARREFOUR_EMAIL', 'manumarder@gmail.com')
        self.password = os.getenv('CARREFOUR_PASSWORD', 'Ipecd2025')
    
    def setup_driver(self):
        """Configura el driver de Selenium"""
        if self.driver is None:
            options = Options()
            # Activar modo headless para producción
            #options.add_argument('--headless')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--ignore-certificate-errors')
            options.add_argument('--ignore-ssl-errors')
            options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
            options.add_argument('--window-size=1920,1080')
            # Optimizaciones de rendimiento
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_experimental_option('excludeSwitches', ['enable-automation'])
            options.add_experimental_option('useAutomationExtension', False)
            
            self.driver = webdriver.Chrome(options=options)
            self.wait = WebDriverWait(self.driver, self.timeout)
        
        return self.driver, self.wait
        
    def extraer_producto(self, url):
        """Extrae datos de un producto individual"""
        try:
            # Asegurar sesión activa UNA SOLA VEZ al inicio
            if not self.sesion_iniciada:
                if not self.asegurar_sesion_activa():
                    logger.error("No se pudo establecer sesión")
                    return None
                
            self.driver.get(url)
            time.sleep(1)  # Reducir tiempo de espera
            logger.info(f"Página cargada: {self.driver.title}")
            
            # Verificar si es página de error 404
            if self._es_pagina_error():
                logger.warning(f"❌ Página no encontrada (404): {url}")
                return {"error_type": "404", "url": url, "titulo": self.driver.title}
            
            # Nombre del producto
            nombre = self._extraer_nombre(self.wait)
            if not nombre:
                logger.warning(f"No se pudo extraer nombre de {url}")
                return {"error_type": "no_name", "url": url, "titulo": self.driver.title}
            
            # Precios
            precio_desc = self._extraer_precio_descuento(self.driver)
            precio_normal = self._extraer_precio_normal(self.driver, precio_desc)
            precio_completo, unidad_text = self._extraer_precio_unidad(self.driver)
            
            # Descuentos
            descuentos = self._extraer_descuentos(self.driver)
            
            return {
                "nombre": nombre,
                "precio_normal": self._limpiar_precio(precio_normal),
                "precio_descuento": self._limpiar_precio(precio_desc),
                "precio_por_unidad": self._limpiar_precio(precio_completo),
                "unidad": unidad_text,
                "descuentos": " | ".join(descuentos) if descuentos else "Ninguno",
                "fecha": datetime.today().strftime("%Y-%m-%d"),
                "supermercado": self.nombre_super,
                "url": url
            }
            
        except Exception as e:
            logger.error(f"Error extrayendo {url}: {str(e)}")
            # En caso de error, resetear sesión
            self.sesion_iniciada = False
            return None
        
    def extraer_lista_productos(self, urls):
        """Extrae múltiples productos manteniendo la misma sesión"""
        resultados = []
        
        # Establecer sesión una sola vez
        if not self.asegurar_sesion_activa():
            logger.error("No se pudo establecer sesión para la extracción")
            return resultados
        
        # Extraer cada producto
        for i, url in enumerate(urls, 1):
            logger.info(f"Extrayendo producto {i}/{len(urls)}")
            producto = self.extraer_producto(url)
            if producto:
                resultados.append(producto)
            
            # Pequeña pausa entre requests
            time.sleep(1)
        
        # Guardar sesión al finalizar
        self.guardar_sesion()
        return resultados
    
    def _extraer_nombre(self, wait):
        """Extrae el nombre del producto"""
        selectores = [
            "h1 .vtex-store-components-3-x-productBrand",
            "h1 span.vtex-store-components-3-x-productBrand",
            "h1.vtex-store-components-3-x-productNameContainer",
            "h1[data-testid='product-name']",
            "h1"
        ]
        
        for selector in selectores:
            try:
                elemento = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                nombre = elemento.text.strip()
                if nombre:
                    return nombre
            except:
                continue
        return None
    
    def _extraer_precio_descuento(self, driver):
        """Extrae precio con descuento"""
        selectores = [
            "span.valtech-carrefourar-product-price-0-x-sellingPriceValue",
            "span.valtech-carrefourar-product-price-0-x-currencyInteger",
            "span.valtech-carrefourar-product-price-0-x-currencyContainer",
            "[data-testid='price-value']"
        ]
        
        return self._buscar_precio(driver, selectores, "0")
    
    def _extraer_precio_normal(self, driver, precio_desc):
        """Extrae precio normal"""
        selectores = [
            "span.valtech-carrefourar-product-price-0-x-listPriceValue",
            ".list-price",
            ".original-price"
        ]
        
        precio = self._buscar_precio(driver, selectores, precio_desc)
        return precio if precio != precio_desc else precio_desc
    
    def _extraer_precio_unidad(self, driver):
        """Extrae precio por unidad - mantiene tu lógica actual"""
        precio_completo = ""
        unidad_text = ""
        
        try:
            # Tu lógica actual de precio por unidad
            contenedor_unidad = driver.find_element(By.CSS_SELECTOR, "div.valtech-carrefourar-dynamic-weight-price-0-x-container")
            
            try:
                precio_elem = contenedor_unidad.find_element(By.CSS_SELECTOR, "span.valtech-carrefourar-dynamic-weight-price-0-x-currencyContainer")
                precio_completo = precio_elem.text.strip()
            except:
                pass
            
            try:
                unidad_elem = contenedor_unidad.find_element(By.CSS_SELECTOR, "span.valtech-carrefourar-dynamic-weight-price-0-x-unit")
                unidad_text = unidad_elem.text.strip()
            except:
                pass
                
        except Exception as e:
            logger.info(f"No se pudo obtener precio por unidad: {e}")
            # Aquí puedes agregar tus intentos alternativos
            
        return precio_completo, unidad_text
    
    def _extraer_descuentos(self, driver):
        """Extrae descuentos con múltiples estrategias"""
        descuentos = []
        
        try:
            # ESTRATEGIA 1: Buscar por selectores CSS específicos
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
                            if texto and len(texto) > 1:  # Evitar textos vacíos o de un solo carácter
                                descuentos.append(texto)
                                logger.debug(f"Descuento encontrado con selector '{selector}': {texto}")
                except:
                    continue
            
            # ESTRATEGIA 2: Buscar por texto que contenga palabras clave de descuento
            palabras_clave = [
                "OFF", "OFF%", "%", "DESCUENTO", "DCTO", "SAVE", "AHORRO", 
                "PROMO", "OFERTA", "2x1", "3x2", "Llevá", "Lleve", "Bonific"
            ]
            
            # Buscar elementos que contengan estas palabras
            for palabra in palabras_clave:
                try:
                    elementos = driver.find_elements(By.XPATH, f"//*[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{palabra.lower()}')]")
                    for elem in elementos:
                        if elem.is_displayed():
                            texto = elem.text.strip()
                            if texto and len(texto) > 1 and any(clave in texto.upper() for clave in palabras_clave):
                                descuentos.append(texto)
                                logger.debug(f"Descuento encontrado por texto '{palabra}': {texto}")
                except:
                    continue

        except Exception as e:
            logger.error(f"Error extrayendo descuentos: {str(e)}")
            
        # Limpiar y filtrar resultados
        descuentos_limpios = []
        for descuento in descuentos:
            # Eliminar duplicados y textos irrelevantes
            if (descuento and 
                    len(descuento) > 1 and 
                    descuento not in descuentos_limpios and
                    not descuento.replace('%', '').replace('$', '').strip().isdigit()):  # No solo números
                    descuentos_limpios.append(descuento)
            
        logger.info(f"Descuentos encontrados: {descuentos_limpios}")
        return descuentos_limpios
    
    def _buscar_precio(self, driver, selectores, default):
        """Busca precio en múltiples selectores"""
        for selector in selectores:
            try:
                elementos = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elementos:
                    texto = elem.text.strip()
                    if texto and any(c.isdigit() for c in texto):
                        return texto
            except:
                continue
        return default
    
    def login_con_email_password(self):
        """Login completo con DEBUGGING DETALLADO"""
        try:
            logger.info("=== DEBUG LOGIN ===")
            
            # Paso 1: Ir a página de login
            logger.info("🔍 Navegando a login...")
            self.driver.get("https://www.carrefour.com.ar/login")
            time.sleep(3)
            
            # TOMAR SCREENSHOT ANTES DE CUALQUIER ACCIÓN
            self.driver.save_screenshot('debug_01_login_page.png')
            logger.info("📸 Screenshot: debug_01_login_page.png")
            
            # Paso 2: Buscar botón de email
            logger.info("🔍 Buscando botón email...")
            if not self.hacer_clic_ingresar_con_mail():
                return False
            
            # Screenshot después del clic
            self.driver.save_screenshot('debug_02_after_email_click.png')
            logger.info("📸 Screenshot: debug_02_after_email_click.png")
            
            # Paso 3: Ingresar credenciales con más verificación
            logger.info("🔍 Ingresando credenciales...")
            if not self.ingresar_credenciales_con_debug():
                return False
            
            # Screenshot después de ingresar credenciales
            self.driver.save_screenshot('debug_03_after_credentials.png')
            logger.info("📸 Screenshot: debug_03_after_credentials.png")
            
            # Paso 4: Verificar login con más detalle
            logger.info("🔍 Verificando login...")
            if self.verificar_sesion_con_debug():
                self.sesion_iniciada = True
                self.guardar_sesion()
                logger.info("✅ LOGIN EXITOSO")
                return True
            else:
                logger.error("❌ LOGIN FALLIDO - Revisar screenshots")
                # Tomar screenshot de error
                self.driver.save_screenshot('debug_04_login_failed.png')
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en login: {e}")
            self.driver.save_screenshot('debug_05_error.png')
            return False
        
    def hacer_clic_ingresar_con_mail(self):
        """Hacer clic específicamente en el botón 'ingresar con mail y contraseña'"""
        try:
            logger.info("🔍 Buscando botón 'ingresar con mail y contraseña'...")
            time.sleep(3)
            
            # ESTRATEGIA 1: Búsqueda exacta por texto completo
            try:
                boton_exacto = self.driver.find_element(
                    By.XPATH, 
                    "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'ingresar con mail y contraseña')]"
                )
                if boton_exacto.is_displayed() and boton_exacto.is_enabled():
                    logger.info("🎯 Botón EXACTO encontrado: 'ingresar con mail y contraseña'")
                    
                    # Scroll para asegurar visibilidad
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", boton_exacto)
                    time.sleep(1)
                    
                    # Intentar clic normal
                    try:
                        boton_exacto.click()
                        logger.info("✅ Clic exitoso en botón exacto")
                        time.sleep(5)
                        return True
                    except Exception as click_error:
                        logger.warning(f"⚠️ Clic normal falló: {click_error}")
                        
                        # Intentar clic JavaScript
                        try:
                            self.driver.execute_script("arguments[0].click();", boton_exacto)
                            logger.info("✅ Clic JS exitoso en botón exacto")
                            time.sleep(5)
                            return True
                        except Exception as js_error:
                            logger.error(f"💥 Clic JS también falló: {js_error}")
                            
            except Exception as e:
                logger.debug(f"Búsqueda exacta falló: {e}")
            
            # ESTRATEGIA 2: Búsqueda por partes del texto (más flexible)
            textos_parciales = [
                "ingresar con mail y contraseña",
                "ingresar con mail",
                "mail y contraseña",
                "ingresar con email y contraseña",
                "ingresar con email"
            ]
            
            for texto in textos_parciales:
                try:
                    boton = self.driver.find_element(
                        By.XPATH, 
                        f"//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{texto.lower()}')]"
                    )
                    if boton.is_displayed() and boton.is_enabled():
                        logger.info(f"🎯 Botón encontrado (parcial): '{texto}'")
                        
                        # Scroll para asegurar visibilidad
                        self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", boton)
                        time.sleep(1)
                        
                        # Intentar clic normal
                        try:
                            boton.click()
                            logger.info(f"✅ Clic exitoso en: '{texto}'")
                            time.sleep(5)
                            return True
                        except Exception as click_error:
                            logger.warning(f"⚠️ Clic normal falló en '{texto}': {click_error}")
                            
                            # Intentar clic JavaScript
                            try:
                                self.driver.execute_script("arguments[0].click();", boton)
                                logger.info(f"✅ Clic JS exitoso en: '{texto}'")
                                time.sleep(5)
                                return True
                            except Exception as js_error:
                                logger.error(f"💥 Clic JS falló en '{texto}': {js_error}")
                                
                except Exception as e:
                    logger.debug(f"Texto parcial '{texto}' no encontrado: {e}")
            
            # ESTRATEGIA 3: Buscar en modales específicos
            try:
                modal = self.driver.find_element(By.XPATH, "//div[contains(@class, 'vtex-login')]")
                botones = modal.find_elements(By.TAG_NAME, "button")
                
                logger.info(f"🔍 Buscando en modal - Botones encontrados: {len(botones)}")
                
                for i, boton in enumerate(botones):
                    texto = boton.text.strip().lower()
                    is_enabled = boton.is_enabled()
                    is_displayed = boton.is_displayed()
                    
                    logger.info(f"Botón {i}: '{texto}' | Habilitado: {is_enabled} | Visible: {is_displayed}")
                    
                    # Buscar específicamente el texto completo o parcial
                    if ('ingresar con mail y contraseña' in texto or 
                        'mail y contraseña' in texto or
                        'ingresar con mail' in texto):
                        
                        if is_enabled and is_displayed:
                            logger.info(f"🎯 Botón modal encontrado: '{texto}'")
                            
                            # Scroll para asegurar visibilidad
                            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", boton)
                            time.sleep(1)
                            
                            # Intentar clic normal
                            try:
                                boton.click()
                                logger.info(f"✅ Clic exitoso en botón modal: '{texto}'")
                                time.sleep(5)
                                return True
                            except Exception as click_error:
                                logger.warning(f"⚠️ Clic modal falló: {click_error}")
                                
                                # Intentar clic JavaScript
                                try:
                                    self.driver.execute_script("arguments[0].click();", boton)
                                    logger.info(f"✅ Clic JS exitoso en botón modal: '{texto}'")
                                    time.sleep(5)
                                    return True
                                except Exception as js_error:
                                    logger.error(f"💥 Clic JS modal falló: {js_error}")
                        else:
                            logger.warning(f"⚠️ Botón modal encontrado pero no usable: '{texto}'")
                            
            except Exception as e:
                logger.debug(f"Búsqueda en modal falló: {e}")
            
            # ESTRATEGIA 4: Búsqueda por atributos específicos
            try:
                selectores_especiales = [
                    "button[data-testid*='email']",
                    "button[data-testid*='mail']",
                    "button[aria-label*='mail']",
                    "button[aria-label*='email']",
                    "button[class*='email']",
                    "button[class*='mail']"
                ]
                
                for selector in selectores_especiales:
                    try:
                        boton = self.driver.find_element(By.CSS_SELECTOR, selector)
                        if boton.is_displayed() and boton.is_enabled():
                            logger.info(f"🎯 Botón por selector especial: {selector}")
                            
                            # Verificar que el texto contenga lo que buscamos
                            texto_boton = boton.text.strip().lower()
                            if any(palabra in texto_boton for palabra in ['mail', 'email', 'ingresar']):
                                
                                # Scroll para asegurar visibilidad
                                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", boton)
                                time.sleep(1)
                                
                                # Intentar clic JavaScript directamente (más confiable)
                                self.driver.execute_script("arguments[0].click();", boton)
                                logger.info(f"✅ Clic JS en botón especial: {selector} - Texto: '{texto_boton}'")
                                time.sleep(5)
                                return True
                                
                    except Exception as e:
                        logger.debug(f"Selector especial {selector} falló: {e}")
                        
            except Exception as e:
                logger.debug(f"Búsqueda por atributos falló: {e}")
            
            # ESTRATEGIA 5: Debugging - Mostrar TODOS los botones de la página
            logger.info("🔍 HACIENDO INVENTARIO DE TODOS LOS BOTONES...")
            try:
                todos_los_botones = self.driver.find_elements(By.TAG_NAME, "button")
                logger.info(f"📋 TOTAL de botones en página: {len(todos_los_botones)}")
                
                for i, boton in enumerate(todos_los_botones):
                    try:
                        texto = boton.text.strip()
                        if texto:  # Solo mostrar botones con texto
                            estado = "HABILITADO" if boton.is_enabled() else "DESHABILITADO"
                            visible = "VISIBLE" if boton.is_displayed() else "OCULTO"
                            logger.info(f"Botón {i}: '{texto}' | {estado} | {visible}")
                    except:
                        pass
                        
            except Exception as e:
                logger.error(f"Error en inventario de botones: {e}")
            
            logger.error("❌ No se pudo encontrar el botón 'ingresar con mail y contraseña'")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error general en búsqueda de botón: {e}")
            return False
        
    def ingresar_credenciales_con_debug(self):
        """Ingresar credenciales con debugging detallado"""
        try:
            logger.info("Ingresando credenciales...")
            time.sleep(3)
            
            # VERIFICAR SI HAY MENSAJES DE ERROR ANTES
            try:
                errores = self.driver.find_elements(By.CSS_SELECTOR, ".error, .alert, .message-error")
                for error in errores:
                    if error.is_displayed():
                        logger.error(f"❌ Error visible: {error.text}")
            except:
                pass
            
            # CAMPO EMAIL - Múltiples selectores y más tiempo
            campo_email = None
            selectores_email = [
                "input[placeholder='Ej.: ejemplo@mail.com']",  # Por placeholder específico
                "input[type='text'][placeholder*='mail']",  # Tipo text con placeholder de mail
                "input[type='email']",
                "input[name='email']",
                "input[placeholder*='mail']",
                "input[placeholder*='Email']",
                "#email",
                "[data-testid='email']"
            ]
            
            for selector in selectores_email:
                try:
                    campo_email = self.wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    logger.info(f"✅ Campo email encontrado con selector: {selector}")
                    break
                except:
                    continue
            
            if not campo_email:
                logger.error("❌ No se pudo encontrar campo email con ningún selector")
                return False
            
            # VERIFICAR SI EL CAMPO ESTÁ HABILITADO
            if not campo_email.is_enabled():
                logger.error("❌ Campo email NO está habilitado")
                return False
            
            campo_email.clear()
            campo_email.send_keys(self.email)
            logger.info("✅ Email ingresado")
            time.sleep(1)
            
            # VERIFICAR SI EL EMAIL SE INGRESÓ CORRECTAMENTE
            valor_email = campo_email.get_attribute('value')
            if valor_email != self.email:
                logger.error(f"❌ Email no se ingresó correctamente. Esperado: {self.email}, Obtenido: {valor_email}")
                return False
            
            # CAMPO CONTRASEÑA - MÚLTIPLES ESTRATEGIAS
            campo_password = None
            selectores = [
                "input[type='password']",
                "input[name='password']",
                "#password",
                "input[placeholder*='contraseña']",
                "input[data-testid='password']"
            ]
            
            for selector in selectores:
                try:
                    campo_password = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if campo_password.is_displayed() and campo_password.is_enabled():
                        logger.info(f"✅ Campo password encontrado: {selector}")
                        break
                except:
                    continue
            
            if not campo_password:
                logger.error("❌ No se pudo encontrar campo contraseña")
                # Listar todos los inputs para debugging
                try:
                    inputs = self.driver.find_elements(By.TAG_NAME, "input")
                    logger.info(f"Inputs encontrados: {len(inputs)}")
                    for i, inp in enumerate(inputs):
                        if inp.is_displayed():
                            tipo = inp.get_attribute('type')
                            name = inp.get_attribute('name')
                            placeholder = inp.get_attribute('placeholder')
                            logger.info(f"Input {i}: type={tipo}, name={name}, placeholder={placeholder}")
                except:
                    pass
                return False
            
            campo_password.clear()
            campo_password.send_keys(self.password)
            logger.info("✅ Contraseña ingresada")
            time.sleep(1)
            
            # VERIFICAR SI LA CONTRASEÑA SE INGRESÓ
            valor_password = campo_password.get_attribute('value')
            if len(valor_password) != len(self.password):
                logger.error("❌ Contraseña no se ingresó correctamente")
                return False
            
            # BOTÓN LOGIN - CON JAVASCRIPT COMO ALTERNATIVA
            try:
                boton_login = self.wait.until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button[type='submit']"))
                )
                logger.info("✅ Botón login encontrado")
                
                # VERIFICAR SI EL BOTÓN ESTÁ HABILITADO
                if not boton_login.is_enabled():
                    logger.error("❌ Botón login NO está habilitado")
                    return False
                
                # HACER CLIC - INTENTAR NORMAL PRIMERO, LUEGO JAVASCRIPT
                try:
                    boton_login.click()
                    logger.info("✅ Clic normal en botón login")
                except Exception as click_error:
                    logger.warning(f"⚠️ Clic normal falló: {click_error}")
                    # Intentar JavaScript click como alternativa
                    try:
                        self.driver.execute_script("arguments[0].click();", boton_login)
                        logger.info("✅ Clic JS en botón login")
                    except Exception as js_error:
                        logger.error(f"❌ Clic JS también falló: {js_error}")
                        return False
                
                time.sleep(5)  # Esperar más tiempo para el login
                return True
                
            except Exception as e:
                logger.error(f"❌ Error con botón login: {e}")
                return False
            
        except Exception as e:
            logger.error(f"❌ Error en credenciales: {e}")
            return False

    def verificar_sesion_con_debug(self):
        """Verificar sesión con debugging detallado"""
        try:
            logger.info("Verificando sesión...")
            
            # Esperar a que se complete cualquier redirección
            time.sleep(3)
            
            # Verificar URL actual
            current_url = self.driver.current_url
            logger.info(f"📋 URL actual: {current_url}")
            
            # Si estamos todavía en login, falló
            if 'login' in current_url.lower():
                logger.error("❌ Seguimos en página de login")
                # Buscar mensajes de error específicos
                try:
                    errores = self.driver.find_elements(By.CSS_SELECTOR, ".error, .alert, .message-error, .vtex-login-2-x-errorMessage")
                    for error in errores:
                        if error.is_displayed():
                            logger.error(f"❌ Mensaje de error: {error.text}")
                except:
                    pass
                return False
            
            # Buscar indicadores de sesión activa
            indicadores = [
                "//*[contains(text(), 'Mi cuenta')]",
                "//*[contains(text(), 'Hola')]",
                "//*[contains(text(), 'Cuenta')]",
                ".my-account",
                "[data-testid='user-menu']"
            ]
            
            for indicador in indicadores:
                try:
                    if indicador.startswith("//"):
                        elemento = self.driver.find_element(By.XPATH, indicador)
                    else:
                        elemento = self.driver.find_element(By.CSS_SELECTOR, indicador)
                    
                    if elemento.is_displayed():
                        logger.info(f"✅ Sesión activa - Indicador: {indicador}")
                        return True
                except:
                    continue
            
            logger.error("❌ No se encontraron indicadores de sesión activa")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error verificando sesión: {e}")
            return False
        
    def asegurar_sesion_activa(self):
        """Asegurar sesión con reintentos limitados"""
        if self.driver is None:
            self.setup_driver()
        
        # Solo 2 intentos máximo
        for intento in range(2):
            logger.info(f"🔄 Intento {intento + 1}/2 de login")
            
            # Intentar cargar sesión existente
            if intento == 0 and os.path.exists(self.cookies_file):
                try:
                    with open(self.cookies_file, 'rb') as f:
                        cookies = pickle.load(f)
                    
                    self.driver.get("https://www.carrefour.com.ar")
                    self.driver.delete_all_cookies()
                    for cookie in cookies:
                        try:
                            self.driver.add_cookie(cookie)
                        except:
                            pass
                    
                    self.driver.refresh()
                    time.sleep(3)
                    
                    if self.verificar_sesion_con_debug():
                        self.sesion_iniciada = True
                        logger.info("✅ Sesión cargada")
                        return True
                except Exception as e:
                    logger.debug(f"Error cargando sesión: {e}")
            
            # Login nuevo
            if self.login_con_email_password():
                return True
            
            # Esperar entre intentos
            if intento < 1:  # No esperar después del último intento
                time.sleep(5)
        
        logger.error("❌ Todos los intentos de login fallaron")
        return False

    def guardar_sesion(self):
        """Guarda las cookies de la sesión actual"""
        try:
            if self.driver and self.sesion_iniciada:
                cookies = self.driver.get_cookies()
                with open(self.cookies_file, 'wb') as f:
                    pickle.dump(cookies, f)
                logger.info(f"Sesión guardada en {self.cookies_file}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error guardando sesión: {e}")
            return False

    def cerrar(self):
        """Método unificado para cerrar driver y limpiar recursos"""
        self.cerrar_driver()

    def _es_pagina_error(self):
        """Detecta si la página actual es una página de error 404"""
        try:
            # Buscar indicadores de página no encontrada
            indicadores_error = [
                "¡Ups!",
                "Página no encontrada", 
                "página no existe",
                "404",
                "no encontrada"
            ]
            
            titulo = self.driver.title.lower()
            body_text = ""
            
            try:
                body_text = self.driver.find_element(By.TAG_NAME, "body").text.lower()
            except:
                pass
            
            # Si el título o el contenido contienen indicadores de error
            for indicador in indicadores_error:
                if indicador.lower() in titulo or indicador.lower() in body_text:
                    return True
                    
            # Verificar si hay elementos específicos de error de Carrefour
            try:
                error_element = self.driver.find_element(By.XPATH, "//*[contains(text(), '¡Ups!') or contains(text(), 'Página no encontrada')]")
                return True
            except:
                pass
                
            return False
        except Exception as e:
            logger.debug(f"Error verificando página de error: {e}")
            return False
    
    def _limpiar_precio(self, precio_texto):
        """Limpia el texto del precio eliminando caracteres no numéricos excepto punto y coma"""
        if not precio_texto:
            return "0"
        
        try:
            # Remover todo excepto números, puntos, comas y el signo $
            import re
            # Mantener solo números, puntos, comas y espacios
            precio_limpio = re.sub(r'[^\d.,\s$]', '', str(precio_texto))
            # Remover espacios extras
            precio_limpio = precio_limpio.strip()
            return precio_limpio if precio_limpio else "0"
        except Exception as e:
            logger.debug(f"Error limpiando precio '{precio_texto}': {e}")
            return "0"

    def cerrar_driver(self):
        """Cierra el driver de Selenium"""
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
                self.wait = None
                self.sesion_iniciada = False
                logger.info("Driver de Selenium cerrado correctamente")
        except Exception as e:
            logger.error(f"Error cerrando driver: {e}")

    #VERIFICACION DE LINKS
    def validar_links_productos(self, urls):
        """
        Valida todos los links antes de la extracción completa
        Retorna: dict con información de validación por producto
        """
        logger.info("🔍 INICIANDO VALIDACIÓN DE LINKS")
        
        # VERIFICAR QUE LO QUE RECIBIMOS SON URLs VÁLIDAS
        logger.info(f"📦 Datos recibidos para validación: {len(urls)} elementos")
        
        # Filtrar solo URLs válidas
        urls_validas = []
        for item in urls:
            if isinstance(item, str) and item.startswith(('http://', 'https://')):
                urls_validas.append(item)
            else:
                logger.warning(f"⚠️ Elemento no es URL válida: {item}")
        
        logger.info(f"🔗 URLs válidas encontradas: {len(urls_validas)}/{len(urls)}")
        
        if not urls_validas:
            logger.error("❌ No hay URLs válidas para validar")
            return {}
        
        # Asegurar sesión activa
        if not self.asegurar_sesion_activa():
            logger.error("No se pudo establecer sesión para validación")
            return {}
        
        resultados_validacion = {}
        
        for i, url in enumerate(urls, 1):
            logger.info(f"📋 Validando link {i}/{len(urls)}: {url}")
            
            resultado = self._validar_link_individual(url, i)
            resultados_validacion[url] = resultado
            
            # Pequeña pausa entre validaciones
            time.sleep(1)
        
        # Mostrar resumen de validación
        self._mostrar_resumen_validacion(resultados_validacion)
        
        return resultados_validacion
    
    def _validar_link_individual(self, url, numero_link):
        """
        Valida un link individual y retorna información detallada
        """
        try:
            # VERIFICAR QUE LA URL ES VÁLIDA ANTES DE NAVEGAR
            if not url.startswith(('http://', 'https://')):
                return {
                    "valido": False,
                    "estado": "URL_INVALIDA",
                    "mensaje": f"URL no válida: {url}",
                    "titulo_pagina": "N/A",
                    "url_final": "N/A"
                }
            
            # Intentar cargar la página con manejo de timeout
            try:
                self.driver.set_page_load_timeout(30)
                self.driver.get(url)
                time.sleep(3)  # Dar más tiempo para cargar
            except Exception as load_error:
                return {
                    "valido": False,
                    "estado": "ERROR_CARGA",
                    "mensaje": f"No se pudo cargar la página: {str(load_error)}",
                    "titulo_pagina": "N/A",
                    "url_final": "N/A"
                }
            
            # DEBUG: Tomar screenshot para análisis
            self.driver.save_screenshot(f'debug_producto_{numero_link}.png')
            logger.info(f"Screenshot guardado: debug_producto_{numero_link}.png")
            
            # Verificar si la página cargó correctamente
            current_url = self.driver.current_url
            titulo_pagina = self.driver.title
            
            logger.info(f"Página cargada - Título: {titulo_pagina}")
            logger.info(f"URL final: {current_url}")
            
            if "carrefour.com.ar" not in current_url:
                return {
                    "valido": False,
                    "estado": "ERROR_CARGA",
                    "mensaje": "No se pudo cargar la página de Carrefour",
                    "titulo_pagina": titulo_pagina,
                    "url_final": current_url
                }
            
            # ESTRATEGIA 1: Verificar si es página de error "¡Ups! Página no encontrada"
            if self._es_pagina_error_ups():
                return {
                    "valido": False,
                    "estado": "PAGINA_NO_ENCONTRADA",
                    "mensaje": "Página no encontrada (Error 404)",
                    "titulo_pagina": titulo_pagina,
                    "url_final": current_url
                }
            
            # ESTRATEGIA 2: Verificar si el producto NO está disponible
            if self._producto_no_disponible():
                return {
                    "valido": False,
                    "estado": "NO_DISPONIBLE",
                    "mensaje": "Producto no disponible o sin stock",
                    "titulo_pagina": titulo_pagina,
                    "url_final": current_url
                }
            
            # ESTRATEGIA 3: Verificar si el producto SÍ está disponible (tiene botón Agregar)
            if self._producto_disponible():
                # Verificar si podemos extraer información básica
                nombre = self._extraer_nombre(self.wait)
                logger.info(f"Nombre extraído: {nombre}")
                
                if not nombre:
                    return {
                        "valido": False,
                        "estado": "SIN_NOMBRE",
                        "mensaje": "No se puede extraer el nombre del producto",
                        "titulo_pagina": titulo_pagina,
                        "url_final": current_url
                    }
                
                # Verificar si hay precios
                precio_desc = self._extraer_precio_descuento(self.driver)
                logger.info(f"Precio extraído: {precio_desc}")
                
                if precio_desc == "0":
                    return {
                        "valido": False,
                        "estado": "SIN_PRECIO",
                        "mensaje": "No se puede extraer precio del producto",
                        "titulo_pagina": titulo_pagina,
                        "nombre_producto": nombre,
                        "url_final": current_url
                    }
                
                # Si llegamos aquí, el link es válido
                return {
                    "valido": True,
                    "estado": "OK",
                    "mensaje": "Link válido - Producto disponible y extraíble",
                    "titulo_pagina": titulo_pagina,
                    "nombre_producto": nombre,
                    "precio_descuento": precio_desc,
                    "url_final": current_url
                }
            
            # Si no cumple ninguna de las condiciones anteriores, es inválido
            return {
                "valido": False,
                "estado": "DESCONOCIDO",
                "mensaje": "No se pudo determinar el estado del producto",
                "titulo_pagina": titulo_pagina,
                "url_final": current_url
            }
            
        except Exception as e:
            logger.error(f"❌ Error validando link {numero_link}: {str(e)}")
            return {
                "valido": False,
                "estado": "ERROR_EXCEPCION",
                "mensaje": f"Error inesperado: {str(e)}",
                "titulo_pagina": self.driver.title if self.driver else "No disponible",
                "url_final": self.driver.current_url if self.driver else "No disponible"
            }
        
    def _es_pagina_error_ups(self):
        """
        Verifica si la página muestra el error "¡Ups! Página no encontrada"
        """
        try:
            # Buscar el texto exacto del error
            textos_error = [
                "¡Ups!",
                "Página no encontrada",
                "página no existe",
                "no encontrada"
            ]
            
            # Verificar en todo el contenido de la página
            page_text = self.driver.page_source
            page_lower = page_text.lower()
            
            for texto in textos_error:
                if texto.lower() in page_lower:
                    logger.info(f"❌ Página de error detectada: '{texto}'")
                    return True
            
            # Buscar elementos específicos del error
            selectores_error = [
                "//*[contains(text(), '¡Ups!')]",
                "//*[contains(text(), 'Página no encontrada')]",
                ".error-page",
                ".not-found",
                "[class*='error']",
                "[class*='not-found']"
            ]
            
            for selector in selectores_error:
                try:
                    if selector.startswith("//"):
                        elemento = self.driver.find_element(By.XPATH, selector)
                    else:
                        elemento = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if elemento.is_displayed():
                        logger.info(f"❌ Elemento de error encontrado: {selector}")
                        return True
                except:
                    continue
                    
            return False
            
        except Exception as e:
            logger.debug(f"Error verificando página de error: {e}")
            return False
        
    def _es_pagina_producto(self):
        """
        Verifica si la página actual es una página de producto individual
        """
        try:
            # Verificar por elementos específicos de página de producto
            indicadores_producto = [
                "//div[contains(@class, 'product-page')]",
                "//div[contains(@class, 'product-details')]",
                "//div[contains(@class, 'vtex-store-components-3-x-productNameContainer')]",
                "//h1[contains(@class, 'product-brand')]",
                "//div[contains(@class, 'valtech-carrefourar-product-price-0-x-sellingPriceValue')]"
            ]
            
            for indicador in indicadores_producto:
                try:
                    elemento = self.driver.find_element(By.XPATH, indicador)
                    if elemento.is_displayed():
                        return True
                except:
                    continue
            
            # Verificar por URL pattern de producto
            if '/p?' in self.driver.current_url or '/producto/' in self.driver.current_url:
                return True
                
            return False
            
        except Exception as e:
            logger.debug(f"Error verificando página producto: {e}")
            return False
    
    def _producto_no_disponible(self):
        """
        Verifica si el producto NO está disponible (tiene botón de no disponible)
        """
        try:
            # Buscar botones o mensajes de no disponibilidad
            textos_no_disponible = [
                "no disponible",
                "sin stock", 
                "producto agotado",
                "no disponible online",
                "fuera de stock",
                "stock agotado"
            ]
            
            # Buscar en botones
            selectores_boton_no_disponible = [
                "//button[contains(., 'No disponible')]",
                "//button[contains(., 'Sin stock')]",
                "//button[contains(., 'Agotado')]",
                "//button[@disabled]",
                "button[disabled]",
                ".out-of-stock",
                ".unavailable",
                ".no-stock"
            ]
            
            # Verificar texto en la página
            page_text = self.driver.page_source.lower()
            for texto in textos_no_disponible:
                if texto in page_text:
                    logger.info(f"❌ Producto no disponible - Texto: '{texto}'")
                    return True
            
            # Verificar botones de no disponible
            for selector in selectores_boton_no_disponible:
                try:
                    if selector.startswith("//"):
                        boton = self.driver.find_element(By.XPATH, selector)
                    else:
                        boton = self.driver.find_element(By.CSS_SELECTOR, selector)
                    
                    if boton.is_displayed():
                        logger.info(f"❌ Botón no disponible encontrado: {selector}")
                        return True
                except:
                    continue
                    
            return False
            
        except Exception as e:
            logger.debug(f"Error verificando no disponibilidad: {e}")
            return False

    def _producto_disponible(self):
        """
        Verifica si el producto SÍ está disponible (tiene botón Agregar)
        """
        try:
            # Buscar botones de agregar al carrito
            textos_agregar = [
                "Agregar",
                "Agregar al carrito", 
                "Comprar",
                "Add to cart",
                "Añadir al carrito"
            ]
            
            selectores_boton_agregar = [
                "//button[contains(., 'Agregar')]",
                "//button[contains(., 'Comprar')]",
                "button[data-testid='buy-button']",
                "button.add-to-cart",
                "button.buy-button",
                ".add-to-cart-button",
                "[class*='add-to-cart']"
            ]
            
            # Verificar botones de agregar
            for texto in textos_agregar:
                try:
                    boton = self.driver.find_element(By.XPATH, f"//button[contains(., '{texto}')]")
                    if boton.is_displayed() and boton.is_enabled():
                        logger.info(f"✅ Botón disponible encontrado: '{texto}'")
                        return True
                except:
                    continue
            
            # Verificar por selectores CSS
            for selector in selectores_boton_agregar:
                try:
                    boton = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if boton.is_displayed() and boton.is_enabled():
                        logger.info(f"✅ Botón disponible encontrado: {selector}")
                        return True
                except:
                    continue
            
            # Si no encontramos botón de agregar, verificar si al menos es página de producto
            # con elementos básicos (nombre y precio)
            try:
                nombre = self._extraer_nombre(self.wait)
                precio = self._extraer_precio_descuento(self.driver)
                
                if nombre and precio != "0":
                    logger.info("✅ Producto disponible (tiene nombre y precio)")
                    return True
            except:
                pass
                
            return False
            
        except Exception as e:
            logger.debug(f"Error verificando disponibilidad: {e}")
            return False
    
    def _mostrar_resumen_validacion(self, resultados):
        """
        Muestra un resumen detallado de la validación
        """
        logger.info("📊 ========== RESUMEN DE VALIDACIÓN ==========")
        
        total_links = len(resultados)
        links_validos = sum(1 for r in resultados.values() if r.get('valido', False))
        links_invalidos = total_links - links_validos
        
        logger.info(f"📈 Total links procesados: {total_links}")
        logger.info(f"✅ Links válidos: {links_validos}")
        logger.info(f"❌ Links inválidos: {links_invalidos}")
        
        # Detalle por estado
        estados = {}
        for resultado in resultados.values():
            estado = resultado.get('estado', 'DESCONOCIDO')
            estados[estado] = estados.get(estado, 0) + 1
        
        logger.info("📋 Detalle por estado:")
        for estado, cantidad in estados.items():
            logger.info(f"   - {estado}: {cantidad} links")
        
        # Mostrar links inválidos específicos
        if links_invalidos > 0:
            logger.info("🔍 Links inválidos detectados:")
            for url, resultado in resultados.items():
                if not resultado.get('valido', False):
                    logger.info(f"   ❌ {url}")
                    logger.info(f"      Estado: {resultado.get('estado', 'N/A')}")
                    logger.info(f"      Mensaje: {resultado.get('mensaje', 'N/A')}")
        
        logger.info("==============================================")