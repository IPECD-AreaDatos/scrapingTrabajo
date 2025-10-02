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
        """Hacer clic con debugging"""
        try:
            logger.info("Buscando botón email...")
            time.sleep(3)
            
            # Estrategia principal: buscar en modal
            try:
                modal = self.driver.find_element(By.XPATH, "//div[contains(@class, 'vtex-login')]")
                botones = modal.find_elements(By.TAG_NAME, "button")
                
                logger.info(f"Botones encontrados: {len(botones)}")
                for i, boton in enumerate(botones):
                    texto = boton.text.strip().lower()
                    is_enabled = boton.is_enabled()
                    is_displayed = boton.is_displayed()
                    logger.info(f"Botón {i}: '{texto}' | Habilitado: {is_enabled} | Visible: {is_displayed}")
                    
                    if ('mail' in texto or 'email' in texto):
                        if is_enabled and is_displayed:
                            logger.info(f"🎯 Intentando clic en: '{texto}'")
                            try:
                                boton.click()
                                logger.info(f"✅ Clic exitoso en botón: '{texto}'")
                                time.sleep(5)  # Más tiempo para cargar formulario
                                return True
                            except Exception as click_error:
                                logger.error(f"💥 Error en clic: {click_error}")
                                # Intentar JavaScript click como alternativa
                                try:
                                    self.driver.execute_script("arguments[0].click();", boton)
                                    logger.info(f"✅ Clic JS exitoso en botón: '{texto}'")
                                    time.sleep(5)  # Más tiempo para cargar formulario
                                    return True
                                except Exception as js_error:
                                    logger.error(f"💥 Error en clic JS: {js_error}")
                        else:
                            logger.warning(f"⚠️ Botón encontrado pero no habilitado/visible: '{texto}'")
            except Exception as e:
                logger.debug(f"Modal falló: {e}")
            
            # Estrategia secundaria
            try:
                boton = self.driver.find_element(By.XPATH, "//button[contains(., 'mail')]")
                boton.click()
                logger.info("✅ Clic en botón mail (XPath)")
                time.sleep(3)
                return True
            except:
                pass
            
            logger.error("❌ No se pudo encontrar botón email")
            return False
            
        except Exception as e:
            logger.error(f"Error en clic email: {e}")
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
            
            # Intentar acceder a página de cuenta
            try:
                self.driver.get("https://www.carrefour.com.ar/minha-conta")
                time.sleep(2)
                if 'login' not in self.driver.current_url.lower():
                    logger.info("✅ Sesión activa - Acceso a cuenta permitido")
                    return True
                else:
                    logger.error("❌ Redirigido a login al acceder a cuenta")
            except:
                pass
            
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