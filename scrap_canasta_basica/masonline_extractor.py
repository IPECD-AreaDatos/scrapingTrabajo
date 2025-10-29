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
logging.basicConfig(level=logging.INFO)
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
            producto = self.extract_product(url)
            if producto:
                resultados.append(producto)
            
            time.sleep(self.CONFIG['wait_between_requests'])
        
        self.guardar_sesion()
        return resultados

    def extract_product(self, url):
        """Extrae datos de un producto individual (ESQUELETO - IMPLEMENTAR DESPUÉS)"""
        try:
            # Asegurar sesión activa
            if not self.sesion_iniciada:
                if not self.asegurar_sesion_activa():
                    logger.error("No se pudo establecer sesión en Masonline")
                    return None
            
            self.driver.get(url)
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
            
            # USAR SELECTORES ESPECÍFICOS PARA CADA PRECIO
            price_normal = self._extract_price_with_selectors([
                "body > div.render-container.render-route-store-product > div > div.vtex-store__template.bg-base > div > div > div > div:nth-child(6) > div > div:nth-child(1) > div.vtex-flex-layout-0-x-flexRow.vtex-flex-layout-0-x-flexRow--cl-mainPv > section > div > div:nth-child(2) > div > div:nth-child(2) > div > div > div > div > div > div:nth-child(1) > div > div:nth-child(1)",
                ".valtech-gdn-dynamic-product-1-x-dynamicProductPrice",
                "[class*='price']"
            ], "precio_normal")
            
            price_discount = self._extract_price_with_selectors([
                "body > div.render-container.render-route-store-product > div > div.vtex-store__template.bg-base > div > div > div > div:nth-child(6) > div > div:nth-child(1) > div.vtex-flex-layout-0-x-flexRow.vtex-flex-layout-0-x-flexRow--cl-mainPv > section > div > div:nth-child(2) > div > div:nth-child(2) > div > div > div > div > div > div:nth-child(1) > div > div:nth-child(1) > div > div.valtech-gdn-dynamic-product-1-x-dynamicProductPrice.mb4.w-100.flex.items-center",
                ".valtech-gdn-dynamic-product-1-x-currencyContainer",
                "[class*='sellingPrice']"
            ], "precio_descuento")

            # Si hay precio de descuento, verificar si es real
            if price_discount and price_normal:
                if not self._has_real_discount(price_normal, price_discount):
                    price_discount = ""
            
            unit_price, unit_text = self._extract_unit_price()
            discounts = self._extract_discounts()
            
            product_data = self._build_product_data(name, price_normal, price_discount, unit_price, unit_text, discounts, url)
            
            final_price = price_discount if price_discount else price_normal
            if final_price:
                logger.info(f"✅ Producto extraído: {name} - Precio: {final_price}")
            else:
                logger.warning(f"⚠️ Producto extraído SIN PRECIO: {name}")
                
            return product_data
            
        except Exception as e:
            logger.error(f"Error extrayendo {url}: {str(e)}")
            self.sesion_iniciada = False
            return None
        
    def _extract_price_with_selectors(self, selectors, price_type):
        """Extrae precio usando selectores específicos"""
        for selector in selectors:
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, selector)
                if element.is_displayed():
                    text = element.text.strip()
                    if text and self._is_valid_price(text):
                        price_text = self._clean_price_text(text)
                        if price_text:
                            logger.debug(f"✅ {price_type} encontrado: {price_text}")
                            return price_text
            except Exception as e:
                logger.debug(f"❌ Selector {price_type} '{selector}' falló: {e}")
                continue
        return ""
        
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
            logger.warning(f"⚠️ Error al limpiar nombre: {str(e)}, usando nombre original")
            return raw_name
    
    def _extract_price_discount(self):
        """Extrae precio con descuento - MÉTODO MEJORADO"""
        return self._search_price(self.SELECTORS['price_discount'], "precio_descuento")
    
    def _has_real_discount(self, price_normal, price_discount):
        """Determina si hay un descuento real"""
        if not price_normal or not price_discount:
            return False
        
        try:
            normal_num = self._extract_numeric_price(price_normal)
            discount_num = self._extract_numeric_price(price_discount)
            
            if normal_num > 0 and discount_num > 0:
                discount_percentage = ((normal_num - discount_num) / normal_num) * 100
                return discount_percentage >= 5
            return False
        except:
            return False

    def _extract_price_normal(self, price_discount):
        """Extrae precio normal - MÉTODO MEJORADO"""
        price_normal = self._search_price(self.SELECTORS['price_normal'], "precio_normal")
        return price_normal if price_normal != price_discount else price_discount
    
    def _extract_unit_price(self):
        """Extrae precio por unidad - MÉTODO MEJORADO"""
        unit_price = ""
        unit_text = ""
        
        # Buscar en el nombre o elementos específicos
        try:
            name_element = self.driver.find_element(By.TAG_NAME, "h1")
            name_text = name_element.text.lower()
            if 'kg' in name_text or 'kilo' in name_text:
                unit_text = "1 kg"
            elif 'g' in name_text or 'gr' in name_text:
                unit_text = "por gramo"
            elif 'ml' in name_text:
                unit_text = "por ml"
            elif 'un' in name_text or 'unidad' in name_text:
                unit_text = "por unidad"
        except:
            pass
        
        return unit_price, unit_text
    
    def _extract_discounts(self):
        """Extrae descuentos - MÉTODO MEJORADO"""
        discounts = []
        
        # Buscar elementos de descuento
        discount_selectors = [
            ".valtech-gdn-dynamic-product-1-x-weightableSavings",
            "[class*='savings']",
            "[class*='discount']"
        ]
        
        for selector in discount_selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    if element.is_displayed():
                        text = element.text.strip()
                        if text and any(keyword in text.upper() for keyword in ['%', 'OFF', 'DCTO', 'DESCUENTO']):
                            discounts.append(text)
            except:
                continue
        
        return list(set(discounts))
    
    def _search_price(self, selectors, price_type):
        """Busca precio usando múltiples estrategias (estilo Delimart)"""
        # Primero buscar en contenedor principal
        main_container = self._find_main_container()
        if main_container:
            price = self._search_in_container(main_container, selectors)
            if price:
                return price
        
        # Si no se encuentra, buscar en toda la página
        logger.debug(f"🔍 Buscando {price_type} en toda la página")
        all_prices = self._search_all_prices(selectors)
        
        if all_prices:
            selected_price = self._select_best_price(all_prices)
            return selected_price
        
        logger.warning(f"❌ No se encontró {price_type} con {len(selectors)} selectores")
        return ""
    
    def _find_main_container(self):
        """Encuentra el contenedor principal del producto"""
        for container in self.SELECTORS['main_container']:
            try:
                element = self.driver.find_element(By.CSS_SELECTOR, container)
                logger.debug(f"✅ Contenedor principal encontrado: {container}")
                return element
            except:
                continue
        return None
    
    def _search_in_container(self, container, selectors):
        """Busca precio dentro de un contenedor específico"""
        for selector in selectors:
            try:
                elements = container.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    if element.is_displayed():
                        text = element.text.strip()
                        if self._is_valid_price(text):
                            logger.debug(f"✅ Precio encontrado en contenedor: '{text}'")
                            return text
            except Exception as e:
                logger.debug(f"❌ Selector '{selector}' falló en contenedor: {e}")
        return None
    
    def _search_all_prices(self, selectors):
        """Busca todos los precios en la página"""
        prices_found = []
        
        for selector in selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    if element.is_displayed():
                        text = element.text.strip()
                        if self._is_valid_price(text):
                            context = self._get_price_context(element)
                            prices_found.append({
                                'text': text,
                                'selector': selector,
                                'context': context,
                                'element': element
                            })
                            logger.debug(f"💰 Precio encontrado: '{text}' (contexto: {context})")
            except Exception as e:
                logger.debug(f"❌ Selector '{selector}' falló: {e}")
        
        return prices_found
    
    def _select_best_price(self, prices_found):
        """Selecciona el precio más probable"""
        if not prices_found:
            return ""
            
        # Priorizar precios del producto principal
        main_prices = [p for p in prices_found if p['context'] == 'producto_principal']
        if main_prices:
            return main_prices[0]['text']
        
        # Heurísticas para selección
        sorted_prices = sorted(prices_found, 
                             key=lambda x: (
                                 0 if 'selling' in x['selector'] else 1,
                                 0 if 'vtex-product-price' in x['selector'] else 1,
                                 len(x['text']),
                             ))
        
        return sorted_prices[0]['text']
    
    def _is_valid_price(self, text):
        """Determina si un texto es un precio válido"""
        if not text:
            return False
        
        # Excluir precios de impuestos
        if any(keyword in text.lower() for keyword in ['impuesto', 'iva', 'sin impuesto']):
            return False
        
        return '$' in text and any(char.isdigit() for char in text) and len(text) <= 20
    
    def _is_tax_price(self, text):
        """Determina si un texto es un precio de impuestos que debemos ignorar"""
        if not text:
            return False
        
        text_lower = text.lower()
        
        # Palabras clave que indican que es precio de impuestos
        tax_keywords = [
            'impuesto', 'impuestos', 'tax', 'iva', 
            'sin impuesto', 'sin impuestos', 'precio sin impuesto',
            'precio sin impuestos', 'nacionales', 'sin iva'
        ]
        
        # Si contiene alguna palabra clave de impuestos, ignorar
        for keyword in tax_keywords:
            if keyword in text_lower:
                logger.debug(f"🔕 Ignorando precio de impuestos: {text}")
                return True
        
        return False

    def _is_valid_discount(self, text):
        """Determina si un texto es un descuento válido (estilo Delimart)"""
        if not text or len(text) > 50:
            return False
        
        text_upper = text.upper()
        has_discount_keywords = any(word in text_upper for word in ['%', 'OFF', 'DCTO', 'DESCUENTO', '2X1', '3X2', 'PROMO', 'OFERTA'])
        has_digits = any(c.isdigit() for c in text)
        
        return has_discount_keywords and has_digits
    
    def _get_price_context(self, element):
        """Obtiene el contexto del precio"""
        try:
            parent_html = element.find_element(By.XPATH, "./..").get_attribute('outerHTML').lower()
            
            if any(word in parent_html for word in ['relacionado', 'recomendado', 'visto', 'frecuente', 'juntos']):
                return "producto_secundario"
            elif any(word in parent_html for word in ['product', 'detail', 'container', 'main']):
                return "producto_principal"
            else:
                return "desconocido"
        except:
            return "desconocido"
    
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
    
    def _clean_price_text(self, text: str):
        """Limpia y formatea el precio"""
        try:
            if not text or '$' not in text:
                return None
            
            after_dollar = text.split('$', 1)[1]
            price_chars = []
            for char in after_dollar:
                if char.isdigit() or char in '.,':
                    price_chars.append(char)
                elif price_chars:
                    break
            
            if price_chars:
                price_str = ''.join(price_chars)
                
                # Formatear consistentemente
                if '.' in price_str and ',' in price_str:
                    parts = price_str.split(',')
                    integer_part = parts[0].replace('.', '')
                    decimal_part = parts[1][:2]
                    return f"${integer_part}.{decimal_part}"
                elif '.' in price_str and price_str.count('.') > 1:
                    return f"${price_str.replace('.', '')}"
                elif ',' in price_str:
                    if len(price_str.split(',')[1]) <= 2:
                        return f"${price_str.replace(',', '.')}"
                    else:
                        return f"${price_str.replace(',', '')}"
                else:
                    return f"${price_str}"
                        
        except Exception as e:
            logger.debug(f"Error limpiando precio: {e}")
        
        return None
    
    def _extract_numeric_price(self, price_text):
        """Convierte precio texto a numérico"""
        try:
            clean = price_text.replace('$', '').replace(' ', '').strip()
            
            if '.' in clean and ',' in clean:
                parts = clean.split(',')
                integer_part = parts[0].replace('.', '')
                clean = f"{integer_part}.{parts[1]}"
            elif '.' in clean and clean.count('.') > 1:
                clean = clean.replace('.', '')
            elif ',' in clean:
                if len(clean.split(',')[1]) == 2:
                    clean = clean.replace(',', '.')
                else:
                    clean = clean.replace(',', '')
            
            return float(clean)
        except:
            return 0.0
        
    def _extract_prices_simplified(self):
        """ENFOQUE MEJORADO PARA EXTRACCIÓN DE PRECIOS EN MASONLINE"""
        price_data = {'normal': '', 'descuento': '', 'final': ''}
        
        try:
            # Estrategia 1: Selectores específicos de Masonline
            masonline_selectors = [
                ".valtech-gdn-dynamic-product-1-x-currencyContainer",
                ".valtech-gdn-dynamic-product-1-x-currencyInteger",
                ".valtech-gdn-dynamic-product-1-x-dynamicProductPrice",
                "[class*='price']",
                "[class*='Price']",
                ".vtex-flex-layout-0-x-flexCol--product-view-prices-container",
                "div[class*='sellingPrice']",
                "div[class*='listPrice']"
            ]
            
            prices_found = []
            
            for selector in masonline_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        try:
                            if element.is_displayed():
                                text = element.text.strip()
                                if text and self._is_valid_price(text) and not self._is_tax_price(text):
                                    price_text = self._clean_price_text(text)
                                    if price_text and price_text not in prices_found:
                                        prices_found.append(price_text)
                                        logger.debug(f"✅ Precio encontrado con selector '{selector}': {price_text}")
                        except:
                            continue
                except Exception as e:
                    logger.debug(f"❌ Selector '{selector}' falló: {e}")
            
            # Estrategia 2: Búsqueda por XPath (excluyendo precios de impuestos)
            xpath_queries = [
                "//*[contains(text(), '$')]",
                "//span[contains(text(), '$')]",
                "//div[contains(text(), '$')]"
            ]
            
            for xpath in xpath_queries:
                try:
                    elements = self.driver.find_elements(By.XPATH, xpath)
                    for element in elements:
                        try:
                            if element.is_displayed():
                                text = element.text.strip()
                                if (text and '$' in text and 
                                    any(char.isdigit() for char in text) and
                                    not self._is_tax_price(text)):
                                    
                                    price_text = self._clean_price_text(text)
                                    if price_text and price_text not in prices_found:
                                        prices_found.append(price_text)
                                        logger.debug(f"✅ Precio encontrado con XPath '{xpath}': {price_text}")
                        except:
                            continue
                except Exception as e:
                    logger.debug(f"❌ XPath '{xpath}' falló: {e}")
            
            logger.info(f"📊 Precios encontrados (sin impuestos): {prices_found}")
            
            # FILTRAR Y SELECCIONAR PRECIOS MÁS INTELIGENTEMENTE
            if prices_found:
                # Convertir a numérico para ordenar
                numeric_prices = []
                for price in prices_found:
                    numeric_value = self._extract_numeric_price(price)
                    numeric_prices.append({
                        'text': price,
                        'numeric': numeric_value
                    })
                
                # Ordenar por valor numérico
                numeric_prices.sort(key=lambda x: x['numeric'])
                
                # Estrategia de selección mejorada para Masonline
                if len(numeric_prices) >= 1:
                    # En Masonline, generalmente solo hay un precio principal (precio normal)
                    # y el "precio sin impuestos" ya lo filtramos
                    
                    if len(numeric_prices) == 1:
                        # Solo un precio encontrado -> es el precio normal
                        price_data['normal'] = numeric_prices[0]['text']
                        price_data['final'] = numeric_prices[0]['text']
                        price_data['descuento'] = ''  # No hay descuento
                        
                    elif len(numeric_prices) >= 2:
                        # Múltiples precios: usar heurísticas para Masonline
                        # En Masonline, el precio más alto suele ser el normal
                        # y si hay otro más bajo, podría ser descuento o precio por unidad
                        
                        main_prices = []
                        unit_prices = []
                        
                        for price_info in numeric_prices:
                            price_val = price_info['numeric']
                            # Considerar como precio principal si está en rango razonable
                            if 500 <= price_val <= 50000:  # Rango amplio para productos
                                main_prices.append(price_info)
                            else:
                                unit_prices.append(price_info)
                        
                        if main_prices:
                            main_prices.sort(key=lambda x: x['numeric'])
                            
                            if len(main_prices) == 1:
                                # Un solo precio principal
                                price_data['normal'] = main_prices[0]['text']
                                price_data['final'] = main_prices[0]['text']
                                price_data['descuento'] = ''
                            else:
                                # Múltiples precios principales: el más alto es normal, el más bajo es descuento
                                price_data['normal'] = main_prices[-1]['text']
                                price_data['descuento'] = main_prices[0]['text']
                                price_data['final'] = main_prices[0]['text']
                        
                        else:
                            # Si no hay precios principales, usar todos
                            price_data['normal'] = numeric_prices[-1]['text']
                            price_data['final'] = numeric_prices[0]['text']
                            if len(numeric_prices) > 1:
                                price_data['descuento'] = numeric_prices[0]['text']
                
                logger.info(f"🎯 Precios procesados - Normal: {price_data['normal']}, Descuento: {price_data['descuento']}")
            
        except Exception as e:
            logger.error(f"🚨 Error crítico en extracción de precios: {str(e)}")
        
        return price_data


    
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