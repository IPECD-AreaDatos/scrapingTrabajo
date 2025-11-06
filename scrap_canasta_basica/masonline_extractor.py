import os
import time
import re
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

load_dotenv()

# Configurar logging
#logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class MasonlineExtractor:
    """Extractor mejorado para Masonline combinando lo mejor de ambos enfoques"""
    
    # Configuraciones centralizadas (estilo Delimart)
    CONFIG = {
        'timeout': 30,
        'wait_between_requests': 1,
        'supermarket_name': 'Masonline',
        'base_url': 'https://www.masonline.com.ar'
    }
    
    def __init__(self):
        self.nombre_super = self.CONFIG['supermarket_name']
        self.timeout = self.CONFIG['timeout']
        self.driver = None
        self.wait = None
        self.sesion_iniciada = False
        self.cookies_file = "masonline_cookies.pkl"
        self.email = os.getenv('MASONLINE_EMAIL', 'manumarder@gmail.com')
        self.password = os.getenv('MASONLINE_PASSWORD', 'Ipecd2025')
    
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
        """Extrae múltiples productos manteniendo la misma sesión"""
        resultados = []
        
        if not self.asegurar_sesion_activa():
            logger.error("No se pudo establecer sesión para la extracción")
            return resultados
        
        for i, url in enumerate(urls, 1):
            logger.info(f"Extrayendo producto {i}/{len(urls)}")
            producto = self.extraer_producto(url)
            if producto:
                resultados.append(producto)
            
            time.sleep(self.CONFIG['wait_between_requests'])
        
        self.guardar_sesion()
        return resultados

    def extraer_producto(self, url):
        """Extrae datos de un producto individual (ESQUELETO - IMPLEMENTAR DESPUÉS)"""
        try:
            # Asegurar sesión activa
            if not self.sesion_iniciada:
                if not self.asegurar_sesion_activa():
                    logger.error("No se pudo establecer sesión en Masonline")
                    return None
            
            self.driver.get(url)
            try:
                self.wait.until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "span.valtech-gdn-dynamic-product-1-x-dynamicProductPrice")
                ))
            except:
                time.sleep(3)
            logger.info(f"Página cargada: {self.driver.title}")
            
            # Verificar si es página de error
            if self._es_pagina_error():
                logger.warning(f"Página no encontrada: {url}")
                return {"error_type": "404", "url": url, "titulo": self.driver.title}
            
            # Extraer datos usando métodos mejorados
            name = self._extract_name()
            if not name:
                logger.warning(f"No se pudo extraer nombre de {url}")
                return {"error_type": "no_name", "url": url, "titulo": self.driver.title}
            
            prices = self._extract_prices()
            unit_price, unit_text = self._extract_unit_price()
            discounts = self._extract_discounts()

            product_data = self._build_product_data(
                name,
                prices["normal"],
                prices["descuento"],
                unit_price,
                unit_text,
                discounts,
                url
            )

            final_price = prices["descuento"] or prices["normal"]
            if final_price:
                logger.info(f"Producto extraído: {name} - Precio final: ${final_price}")
            else:
                logger.warning(f"Producto sin precio detectado: {name}")

            return product_data

        except Exception as e:
            logger.error(f"Error extrayendo {url}: {e}")
            self.sesion_iniciada = False
            return None
        
        
    def _extract_name(self):
        """Extrae el nombre del producto"""
        selectors = [
            "h1.vtex-store-components-3-x-productNameContainer",
            "h1.vtex-store-components-3-x-productBrand", 
            "h1[data-testid='product-name']",
            "h1"
        ]
        
        for selector in selectors:
            try:
                element = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                name = element.text.strip()
                if name:
                    return name
            except:
                continue
        
        # Fallback al título
        try:
            title = self.driver.title
            if ' - Masonline' in title:
                return title.split(' - Masonline')[0].strip()
            return title
        except:
            return "Producto sin nombre"
    
    def _clean_name(self, raw_name):
        """Limpia el nombre del producto (estilo Delimart)"""
        try:
            lines = raw_name.split('\n')
            
            if len(lines) >= 2:
                # Buscar línea más relevante
                for line in lines:
                    clean_line = line.strip()
                    if clean_line and len(clean_line) > 3:  # Línea con contenido significativo
                        return clean_line
                
                # Si no encuentra patrones claros, tomar la primera línea no vacía
                for line in lines:
                    if line.strip():
                        return line.strip()
            else:
                return raw_name
                
        except Exception as e:
            logger.warning(f" Error al limpiar nombre: {str(e)}, usando nombre original")
            return raw_name
        
    # ==================================================================================
    # MÉTODOS A REEMPLAZAR EN TU CÓDIGO MASONLINE_EXTRACTOR
    # ==================================================================================
    # Solo copia estos 2 métodos y reemplaza los que tienes actualmente
    # NO toques nada más, el login queda intacto
    # ==================================================================================

    def _extract_prices(self):
        """
        Extrae precios para Masonline
        LÓGICA:
        - precio_normal = precio de lista (siempre presente)
        - precio_descuento = precio promocional SI EXISTE, sino igual al precio_normal
        """
        try:
            precio_lista = None
            precio_oferta = None
            
            # ═══════════════════════════════════════════════════════════
            # PASO 1: Buscar PRECIO DE LISTA (precio tachado/original)
            # ═══════════════════════════════════════════════════════════
            selectors_lista = [
                "span.mt4.valtech-gdn-dynamic-product-1-x-weighableListPrice",
                "span.valtech-gdn-dynamic-product-1-x-weighableListPrice",
                "span[class*='listPrice']",
                "span[class*='ListPrice']",
                "s",
                "del"
            ]
            
            for selector in selectors_lista:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element.is_displayed():
                        text = element.text.strip()
                        if text and '$' in text:
                            logger.info(f"Precio lista encontrado con selector '{selector}': '{text}'")
                            precio = self._parsear_precio(text)
                            if precio:
                                precio_lista = precio
                                break
                except:
                    continue
            
            # ═══════════════════════════════════════════════════════════
            # PASO 2: Buscar PRECIO DE OFERTA (precio principal/destacado)
            # ═══════════════════════════════════════════════════════════
            selectors_oferta = [
                "span.valtech-gdn-dynamic-product-1-x-dynamicProductPrice",
                "div.valtech-gdn-dynamic-product-1-x-dynamicProductPrice",
                "span[class*='sellingPrice']",
                "span[class*='SellingPrice']"
            ]
            
            for selector in selectors_oferta:
                try:
                    element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if element.is_displayed():
                        text = element.text.strip()
                        if text and '$' in text:
                            logger.info(f"Precio oferta encontrado con selector '{selector}': '{text}'")
                            precio = self._parsear_precio(text)
                            if precio:
                                precio_oferta = precio
                                break
                except:
                    continue
            
            # ═══════════════════════════════════════════════════════════
            # PASO 3: FALLBACK - Buscar todos los precios visibles
            # ═══════════════════════════════════════════════════════════
            if not precio_lista and not precio_oferta:
                logger.warning("No se encontraron precios con selectores específicos, buscando en todo el DOM")
                todos_precios = self._buscar_todos_precios_visibles()
                
                if todos_precios:
                    logger.info(f"Precios encontrados en DOM: {todos_precios}")
                    if len(todos_precios) >= 2:
                        precio_lista = max(todos_precios)
                        precio_oferta = min(todos_precios)
                    elif len(todos_precios) == 1:
                        precio_lista = todos_precios[0]
                        precio_oferta = None
            
            # ═══════════════════════════════════════════════════════════
            # PASO 4: APLICAR LÓGICA DE NEGOCIO
            # ═══════════════════════════════════════════════════════════
            
            if precio_oferta and not precio_lista:
                logger.warning("Solo se encontró precio de oferta, asumiendo que es el precio normal")
                precio_lista = precio_oferta
            
            if precio_lista and not precio_oferta:
                logger.info("Producto sin oferta detectado")
                precio_oferta = None
            
            if precio_lista and precio_oferta:
                if precio_oferta > precio_lista:
                    logger.warning(f"¡Inconsistencia! Precio oferta ({precio_oferta}) > precio lista ({precio_lista}). Intercambiando...")
                    precio_lista, precio_oferta = precio_oferta, precio_lista
            
            # ═══════════════════════════════════════════════════════════
            # PASO 5: CONSTRUIR RESPUESTA
            # ═══════════════════════════════════════════════════════════
            precio_normal = precio_lista
            precio_descuento = precio_oferta if precio_oferta else precio_normal
            
            logger.info(f"═══ RESULTADO FINAL ═══")
            logger.info(f"Precio Normal (lista): ${precio_normal}")
            logger.info(f"Precio Descuento: ${precio_descuento}")
            if precio_oferta and precio_lista and precio_oferta < precio_lista:
                descuento_pct = ((precio_lista - precio_oferta) / precio_lista) * 100
                logger.info(f"Descuento aplicado: {descuento_pct:.1f}%")
            else:
                logger.info(f"Sin descuento - ambos precios son iguales")
            logger.info(f"═══════════════════════")
            
            return {
                "normal": precio_normal,
                "descuento": precio_descuento
            }
            
        except Exception as e:
            logger.error(f"Error crítico extrayendo precios: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"normal": None, "descuento": None}


    def _parsear_precio(self, precio_str):
        """
        Parsea precios en formato argentino de Masonline
        Ejemplos:
        - "$ 2.999" -> 2999.00
        - "$ 5.649" -> 5649.00
        - "$ 3.38940" -> 3389.40
        - "$ 1.780,35" -> 1780.35
        - "$ 2.824,50" -> 2824.50
        """
        try:
            if not precio_str:
                return None
            
            texto_limpio = precio_str.strip()
            
            if '$' not in texto_limpio:
                return None
            
            # Remover el símbolo $ y espacios
            texto_limpio = texto_limpio.replace('$', '').strip()
            
            # CASO 1: Formato "1.780,35" (punto para miles, coma para decimales)
            if ',' in texto_limpio and '.' in texto_limpio:
                texto_limpio = texto_limpio.replace('.', '').replace(',', '.')
                precio = float(texto_limpio)
                if 1 <= precio <= 100000:
                    logger.debug(f"Parseado con formato miles+decimales: {precio}")
                    return precio
            
            # CASO 2: Formato con punto
            elif '.' in texto_limpio and texto_limpio.count('.') == 1:
                partes = texto_limpio.split('.')
                digitos_despues = len(partes[1])
                
                # SUBCASO 2A: 3 dígitos después del punto = separador de miles
                if digitos_despues == 3:
                    precio = float(texto_limpio.replace('.', ''))
                    if 1 <= precio <= 100000:
                        logger.debug(f"Parseado como miles (3 dígitos): {precio}")
                        return precio
                
                # SUBCASO 2B: >3 dígitos después del punto = formato mal formateado
                elif digitos_despues > 3:
                    numero_completo = partes[0] + partes[1]
                    if len(numero_completo) > 2:
                        entero = numero_completo[:-2]
                        decimal = numero_completo[-2:]
                        precio = float(f"{entero}.{decimal}")
                    else:
                        precio = float(f"0.{numero_completo}")
                    
                    if 1 <= precio <= 100000:
                        logger.debug(f"Parseado decimal extendido: {precio}")
                        return precio
                
                # SUBCASO 2C: ≤2 dígitos después del punto
                else:
                    precio = float(texto_limpio)
                    if 1 <= precio <= 100000:
                        logger.debug(f"Parseado con punto: {precio}")
                        return precio
            
            # CASO 3: Solo coma
            elif ',' in texto_limpio:
                texto_limpio = texto_limpio.replace(',', '.')
                precio = float(texto_limpio)
                if 1 <= precio <= 100000:
                    logger.debug(f"Parseado con coma: {precio}")
                    return precio
            
            # CASO 4: Solo entero
            else:
                precio = float(texto_limpio)
                if 1 <= precio <= 100000:
                    logger.debug(f"Parseado como entero: {precio}")
                    return precio
            
            logger.warning(f"No se pudo parsear: '{precio_str}'")
            return None
            
        except Exception as e:
            logger.debug(f"Error parseando '{precio_str}': {e}")
            return None


    def _buscar_todos_precios_visibles(self):
        """Busca todos los precios visibles como fallback"""
        try:
            precios_encontrados = set()
            elementos = self.driver.find_elements(By.XPATH, "//*[contains(text(), '$')]")
            
            for elemento in elementos:
                try:
                    if not elemento.is_displayed():
                        continue
                    
                    texto = elemento.text.strip()
                    
                    if len(texto) > 50:
                        continue
                    
                    texto_lower = texto.lower()
                    palabras_excluir = ['impuesto', 'kilo', 'kg', 'unidad', 'ahorra', 'x ', 'litro']
                    if any(palabra in texto_lower for palabra in palabras_excluir):
                        continue
                    
                    precio = self._parsear_precio(texto)
                    if precio:
                        precios_encontrados.add(precio)
                        
                except:
                    continue
            
            lista_precios = sorted(list(precios_encontrados), reverse=True)
            
            precios_filtrados = []
            for precio in lista_precios:
                es_duplicado = False
                for precio_existente in precios_filtrados:
                    diferencia_pct = abs(precio - precio_existente) / precio_existente
                    if diferencia_pct < 0.01:
                        es_duplicado = True
                        break
                if not es_duplicado:
                    precios_filtrados.append(precio)
            
            logger.info(f"Precios únicos encontrados: {precios_filtrados}")
            return precios_filtrados
            
        except Exception as e:
            logger.error(f"Error buscando precios visibles: {e}")
            return []
    
    
    def _extract_unit_price(self):
        """Busca precios unitarios (por kg, lt, etc.)."""
        try:
            el = self.driver.find_element(By.XPATH, "//*[contains(text(), 'x')]")
            txt = el.text.strip()
            match = re.search(r"\$[\d\.,]+\s*x\s*\w+", txt)
            return (txt, match.group(0)) if match else (None, None)
        except:
            return (None, None)
        
    def _extract_discounts(self):
        """Detecta textos de promociones o descuentos visibles."""
        try:
            promos = []
            discount_selectors = [
                "//*[contains(text(),'Descuento')]",
                "//*[contains(text(),'Promo')]",
                "//*[contains(text(),'Oferta')]",
                "//*[contains(text(),'%')]"
            ]
            
            for selector in discount_selectors:
                elements = self.driver.find_elements(By.XPATH, selector)
                for el in elements:
                    text = el.text.strip()
                    if text and len(text) < 100:  # Evitar textos muy largos
                        promos.append(text)
            return promos
        except:
            return []
        
    def _build_product_data(self, name, price_normal, price_discount, unit_price, unit_text, discounts, url):
        """Construye los datos del producto"""
        return {
            "nombre": name,
            "precio_normal": price_normal if price_normal else "",
            "precio_descuento": price_discount if price_discount else "",
            "precio_por_unidad": unit_price if unit_price else "",
            "unidad": unit_text if unit_text else "",
            "descuentos": " | ".join(discounts) if discounts else "Ninguno",
            "fecha": datetime.today().strftime("%Y-%m-%d"),
            "supermercado": self.CONFIG['supermarket_name'],
            "url": url
        }


    


    def login_con_email_password(self):
        """Login completo con DEBUGGING DETALLADO para Masonline - VERSIÓN MEJORADA"""
        try:
            logger.info("=== DEBUG LOGIN MASONLINE ===")
            
            # Paso 1: Ir a página de login
            logger.info("🔍 Navegando a login de Masonline...")
            self.driver.get(f"{self.CONFIG['base_url']}/login")
            time.sleep(3)
            
            # TOMAR SCREENSHOT ANTES DE CUALQUIER ACCIÓN
            self.driver.save_screenshot('masonline_debug_01_login_page.png')
            logger.info("📸 Screenshot: masonline_debug_01_login_page.png")
            
            # DEBUG: Mostrar estructura de la página
            logger.info("🔍 Estructura inicial de la página:")
            try:
                h3_elements = self.driver.find_elements(By.TAG_NAME, "h3")
                for i, h3 in enumerate(h3_elements):
                    if h3.is_displayed():
                        logger.info(f"H3 {i}: '{h3.text}'")
            except:
                pass
            
            # Paso 2: Ingresar credenciales
            logger.info("🔍 Ingresando credenciales...")
            if not self.ingresar_credenciales_con_debug():
                return False
            
            # Screenshot después de ingresar credenciales
            self.driver.save_screenshot('masonline_debug_02_after_credentials.png')
            logger.info("📸 Screenshot: masonline_debug_02_after_credentials.png")
            
            # Paso 3: Verificar login
            logger.info("🔍 Verificando login...")
            if self.verificar_sesion_con_debug():
                self.sesion_iniciada = True
                self.guardar_sesion()
                logger.info("✅ LOGIN MASONLINE EXITOSO")
                return True
            else:
                logger.error("❌ LOGIN MASONLINE FALLIDO")
                self.driver.save_screenshot('masonline_debug_03_login_failed.png')
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en login Masonline: {e}")
            self.driver.save_screenshot('masonline_debug_04_error.png')
            return False
    
    def ingresar_credenciales_con_debug(self):
        """Ingresar credenciales en Masonline con debugging - VERSIÓN CORREGIDA"""
        try:
            logger.info("Ingresando credenciales en Masonline...")
            time.sleep(3)
            
            # PRIMERO: HACER CLIC EN "ENTRAR CON E-MAIL Y CONTRASEÑA"
            logger.info("🔍 Buscando opción 'Entrar con e-mail y contraseña'...")
            
            opciones_login = [
                "//h3[contains(text(), 'Entrar con e-mail y contraseña')]",
                "//*[contains(text(), 'Entrar con e-mail y contraseña')]",
                "//button[contains(text(), 'Entrar con e-mail y contraseña')]",
                "//div[contains(text(), 'Entrar con e-mail y contraseña')]"
            ]
            
            opcion_encontrada = False
            for opcion in opciones_login:
                try:
                    elemento = self.driver.find_element(By.XPATH, opcion)
                    if elemento.is_displayed() and elemento.is_enabled():
                        logger.info(f"✅ Opción encontrada: {opcion}")
                        
                        # Hacer clic en la opción
                        try:
                            elemento.click()
                            logger.info("✅ Clic en opción 'Entrar con e-mail y contraseña'")
                            opcion_encontrada = True
                            time.sleep(2)  # Esperar que se despliegue el formulario
                            break
                        except Exception as click_error:
                            logger.warning(f"⚠️ Clic normal falló: {click_error}")
                            try:
                                self.driver.execute_script("arguments[0].click();", elemento)
                                logger.info("✅ Clic JS en opción")
                                opcion_encontrada = True
                                time.sleep(2)
                                break
                            except Exception as js_error:
                                logger.error(f"❌ Clic JS falló: {js_error}")
                except Exception as e:
                    logger.debug(f"Opción {opcion} no encontrada: {e}")
            
            if not opcion_encontrada:
                logger.warning("⚠️ No se encontró la opción específica, intentando continuar...")
            
            # VERIFICAR ESTRUCTURA DE LA PÁGINA
            logger.info("🔍 Analizando estructura de login...")
            try:
                forms = self.driver.find_elements(By.TAG_NAME, "form")
                logger.info(f"Formularios encontrados: {len(forms)}")
                for i, form in enumerate(forms):
                    logger.info(f"Form {i}: {form.get_attribute('id') or form.get_attribute('class')}")
            except Exception as e:
                logger.debug(f"Error analizando forms: {e}")
            
            # CAMPO EMAIL - Selectores específicos para Masonline
            campo_email = None
            selectores_email = [
                "input[placeholder='Ej.: ejemplo@mail.com']",  # ESPECÍFICO de Masonline
                "input[type='email']",
                "input[name='email']", 
                "input[placeholder*='email']",
                "input[placeholder*='Email']",
                "input[placeholder*='mail']",
                "#email",
                ".email-input",
                "input[data-testid='email']",
                "input[id*='email']"
            ]
            
            for selector in selectores_email:
                try:
                    campo_email = self.wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    if campo_email.is_displayed() and campo_email.is_enabled():
                        logger.info(f"✅ Campo email encontrado: {selector}")
                        break
                    else:
                        campo_email = None
                except:
                    continue
            
            if not campo_email:
                logger.error("❌ No se pudo encontrar campo email")
                # Debug: mostrar todos los inputs
                try:
                    inputs = self.driver.find_elements(By.TAG_NAME, "input")
                    logger.info(f"Inputs totales: {len(inputs)}")
                    for i, inp in enumerate(inputs):
                        if inp.is_displayed():
                            logger.info(f"Input {i}: type={inp.get_attribute('type')}, name={inp.get_attribute('name')}, placeholder={inp.get_attribute('placeholder')}")
                except Exception as e:
                    logger.debug(f"Error en debug inputs: {e}")
                return False
            
            # INGRESAR EMAIL
            campo_email.clear()
            campo_email.send_keys(self.email)
            logger.info("✅ Email ingresado")
            time.sleep(1)
            
            # VERIFICAR EMAIL INGRESADO
            valor_email = campo_email.get_attribute('value')
            if valor_email != self.email:
                logger.error(f"❌ Email no se ingresó correctamente: {valor_email}")
                return False
            
            # CAMPO PASSWORD
            campo_password = None
            selectores_password = [
                "input[type='password']",
                "input[name='password']",
                "#password", 
                "input[placeholder*='contraseña']",
                "input[placeholder*='password']",
                ".password-input",
                "input[data-testid='password']",
                "input[id*='password']"
            ]
            
            for selector in selectores_password:
                try:
                    campo_password = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if campo_password.is_displayed() and campo_password.is_enabled():
                        logger.info(f"✅ Campo password encontrado: {selector}")
                        break
                    else:
                        campo_password = None
                except:
                    continue
            
            if not campo_password:
                logger.error("❌ No se pudo encontrar campo password")
                return False
            
            campo_password.clear()
            campo_password.send_keys(self.password)
            logger.info("✅ Contraseña ingresada")
            time.sleep(1)
            
            # BOTÓN LOGIN
            boton_login = None
            selectores_boton = [
                "button[type='submit']",
                "button[class*='login']",
                "button[class*='submit']",
                "input[type='submit']",
                ".login-button",
                ".submit-button",
                "button[data-testid='login']",
                "//button[contains(text(), 'Entrar')]",
                "//button[contains(text(), 'Ingresar')]"
            ]
            
            for selector in selectores_boton:
                try:
                    if selector.startswith("//"):
                        boton_login = self.driver.find_element(By.XPATH, selector)
                    else:
                        boton_login = self.driver.find_element(By.CSS_SELECTOR, selector)
                        
                    if boton_login.is_displayed() and boton_login.is_enabled():
                        logger.info(f"✅ Botón login encontrado: {selector}")
                        break
                    else:
                        boton_login = None
                except:
                    continue
            
            if not boton_login:
                logger.error("❌ No se pudo encontrar botón login")
                # Mostrar todos los botones para debug
                try:
                    botones = self.driver.find_elements(By.TAG_NAME, "button")
                    logger.info(f"Botones totales: {len(botones)}")
                    for i, btn in enumerate(botones):
                        if btn.is_displayed():
                            logger.info(f"Botón {i}: text='{btn.text}', type={btn.get_attribute('type')}")
                except:
                    pass
                return False
            
            # HACER CLIC EN LOGIN
            try:
                boton_login.click()
                logger.info("✅ Clic en botón login")
            except Exception as e:
                logger.warning(f"⚠️ Clic normal falló: {e}")
                try:
                    self.driver.execute_script("arguments[0].click();", boton_login)
                    logger.info("✅ Clic JS en botón login")
                except Exception as js_error:
                    logger.error(f"❌ Clic JS falló: {js_error}")
                    return False
            
            time.sleep(5)  # Esperar proceso de login
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en credenciales Masonline: {e}")
            return False
    
    def verificar_sesion_con_debug(self):
        """Verificar sesión en Masonline con debugging - VERSIÓN MEJORADA"""
        try:
            logger.info("Verificando sesión Masonline...")
            time.sleep(5)  # Más tiempo para redirección
            
            # Verificar URL actual
            current_url = self.driver.current_url
            logger.info(f"📋 URL actual: {current_url}")
            
            # Si estamos en login todavía, falló
            if 'login' in current_url.lower():
                logger.error("❌ Seguimos en página de login")
                
                # Buscar mensajes de error específicos
                try:
                    errores = self.driver.find_elements(By.CSS_SELECTOR, ".error, .alert, .message-error, .vtex-login-2-x-errorMessage")
                    for error in errores:
                        if error.is_displayed():
                            error_text = error.text.strip()
                            logger.error(f"❌ Mensaje de error: {error_text}")
                            
                            # Tomar screenshot del error
                            self.driver.save_screenshot('masonline_login_error.png')
                            logger.info("📸 Screenshot del error: masonline_login_error.png")
                except:
                    pass
                
                # Verificar si hay mensajes de credenciales incorrectas
                page_text = self.driver.find_element(By.TAG_NAME, "body").text.lower()
                if "incorrect" in page_text or "inválid" in page_text or "error" in page_text:
                    logger.error("❌ Posibles credenciales incorrectas")
                
                return False
            
            # Buscar indicadores de sesión activa en Masonline
            indicadores = [
                "//*[contains(text(), 'Mi cuenta')]",
                "//*[contains(text(), 'Hola')]", 
                "//*[contains(text(), 'Bienvenido')]",
                "//*[contains(text(), 'Cerrar sesión')]",
                "//*[contains(text(), 'Mis pedidos')]",
                ".my-account",
                ".user-menu",
                "[data-testid='user-info']",
                "//a[contains(@href, 'my-account')]"
            ]
            
            for indicador in indicadores:
                try:
                    if indicador.startswith("//"):
                        elemento = self.driver.find_element(By.XPATH, indicador)
                    else:
                        elemento = self.driver.find_element(By.CSS_SELECTOR, indicador)
                    
                    if elemento.is_displayed():
                        logger.info(f"✅ Sesión Masonline activa - Indicador: {indicador}")
                        return True
                except:
                    continue
            
            # Verificar si podemos acceder a página de perfil
            try:
                self.driver.get(f"{self.CONFIG['base_url']}/my-account")
                time.sleep(3)
                current_url_after = self.driver.current_url
                logger.info(f"📋 URL después de my-account: {current_url_after}")
                
                if 'login' not in current_url_after.lower() and 'my-account' in current_url_after.lower():
                    logger.info("✅ Acceso a my-account exitoso")
                    return True
            except Exception as e:
                logger.debug(f"Error accediendo a my-account: {e}")
            
            # Última verificación: intentar navegar a página protegida
            try:
                self.driver.get(f"{self.CONFIG['base_url']}/orders")
                time.sleep(2)
                if 'login' not in self.driver.current_url.lower():
                    logger.info("✅ Acceso a orders exitoso")
                    return True
            except:
                pass
            
            logger.error("❌ No se encontraron indicadores de sesión activa en Masonline")
            return False
            
        except Exception as e:
            logger.error(f"❌ Error verificando sesión Masonline: {e}")
            return False
    
    def asegurar_sesion_activa(self):
        """Asegurar sesión con reintentos limitados"""
        if self.driver is None:
            self.setup_driver()
        
        for intento in range(2):
            logger.info(f"🔄 Intento {intento + 1}/2 de login Masonline")
            
            # Intentar cargar sesión existente
            if intento == 0 and os.path.exists(self.cookies_file):
                try:
                    with open(self.cookies_file, 'rb') as f:
                        cookies = pickle.load(f)
                    
                    self.driver.get(self.CONFIG['base_url'])
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
                        logger.info("✅ Sesión Masonline cargada")
                        return True
                except Exception as e:
                    logger.debug(f"Error cargando sesión Masonline: {e}")
            
            # Login nuevo
            if self.login_con_email_password():
                return True
            
            if intento < 1:
                time.sleep(5)
        
        logger.error("❌ Todos los intentos de login Masonline fallaron")
        return False
    
    def guardar_sesion(self):
        """Guarda las cookies de la sesión actual"""
        try:
            if self.driver and self.sesion_iniciada:
                cookies = self.driver.get_cookies()
                with open(self.cookies_file, 'wb') as f:
                    pickle.dump(cookies, f)
                logger.info(f"Sesión Masonline guardada en {self.cookies_file}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error guardando sesión Masonline: {e}")
            return False
    
    def _es_pagina_error(self):
        """Detecta si la página actual es una página de error"""
        try:
            indicadores_error = ["404", "página no encontrada", "error", "no existe", "not found"]
            titulo = self.driver.title.lower()
            body_text = self.driver.find_element(By.TAG_NAME, "body").text.lower()
            
            for indicador in indicadores_error:
                if indicador in titulo or indicador in body_text:
                    return True
                    
            return False
        except:
            return False
    
    def cleanup_driver(self):
        """Cierra el driver de Selenium"""
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
                self.wait = None
                self.sesion_iniciada = False
                logger.info("Driver de Masonline cerrado correctamente")
        except Exception as e:
            logger.error(f"Error cerrando driver Masonline: {e}")