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
import traceback
import re


# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv()

class ExtractorDelimart:
    """Extractor específico para Delimart"""
    
    def __init__(self):
        self.nombre_super = "Delimart"
        self.timeout = 30  # Aumentar timeout para mejor estabilidad
        self.driver = None
        self.wait = None
    
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
            # VERIFICAR QUE EL DRIVER EXISTA
            if self.driver is None:
                logger.error("❌ Driver no inicializado")
                return None
                
            logger.info(f"🌐 Navegando a: {url}")
            self.driver.get(url)
            time.sleep(2)  # Aumentar tiempo para asegurar carga
            logger.info(f"📄 Página cargada: {self.driver.title}")
            
            
            # Nombre del producto con más logging
            logger.info("🔍 Buscando nombre del producto...")
            nombre = self._extraer_nombre(self.wait)
            if not nombre:
                logger.warning(f"❌ No se pudo extraer nombre de {url}")
                # Tomar screenshot para debug
                try:
                    self.driver.save_screenshot(f"error_nombre_{datetime.now().strftime('%H%M%S')}.png")
                    logger.info("📸 Screenshot guardado para debug")
                except:
                    pass
                return {"error_type": "no_name", "url": url, "titulo": self.driver.title}
            
            logger.info(f"✅ Nombre encontrado: {nombre}")
            
            # Precios con logging
            logger.info("🔍 Buscando precios...")
            precio_desc = self._extraer_precio_descuento(self.driver)
            precio_normal = self._extraer_precio_normal(self.driver, precio_desc)
            precio_completo, unidad_text = self._extraer_precio_unidad(self.driver)
            
            logger.info(f"💰 Precios - Normal: {precio_normal}, Descuento: {precio_desc}, Unidad: {precio_completo}")
            
            # Descuentos
            descuentos = self._extraer_descuentos(self.driver)
            logger.info(f"🎯 Descuentos encontrados: {len(descuentos)}")
            
            producto_data = {
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
            
            logger.info(f"🎉 Producto extraído exitosamente: {nombre}")
            return producto_data
            
        except Exception as e:
            logger.error(f"❌ Error extrayendo {url}: {str(e)}")
            logger.error(traceback.format_exc())  # Mostrar traceback completo
            return None
        
    def extraer_lista_productos(self, urls):
        """Extrae múltiples productos manteniendo la misma sesión"""
        resultados = []

        # INICIALIZAR DRIVER ANTES DE EXTRAER
        self.setup_driver()
        
        # Extraer cada producto
        for i, url in enumerate(urls, 1):
            logger.info(f"Extrayendo producto {i}/{len(urls)}")
            producto = self.extraer_producto(url)
            if producto:
                resultados.append(producto)
            
            # Pequeña pausa entre requests
            time.sleep(1)

        # CERRAR DRIVER AL FINALIZAR
        if self.driver:
            self.driver.quit()
            self.driver = None

        # Siempre retornar DataFrame
        return pd.DataFrame(resultados) if resultados else pd.DataFrame()
        
    
    def _extraer_nombre(self, wait):
        """Extrae el nombre del producto con debug"""
        selectores = [
            ".product-name",  # Selector específico según el screenshot
            "h1", 
            "[class*='product-name']",
            "h1 .vtex-store-components-3-x-productBrand"
        ]
        
        for i, selector in enumerate(selectores):
            try:
                logger.debug(f"🔍 Probando selector {i+1}: {selector}")
                elemento = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
                nombre = elemento.text.strip()
                if nombre:
                    logger.info(f"✅ Nombre encontrado con selector {i+1}: {nombre}")
                    return nombre
            except Exception as e:
                logger.debug(f"❌ Selector {i+1} falló: {str(e)}")
                continue
        
        logger.error("❌ No se pudo encontrar el nombre con ningún selector")
        return None
    
    def _extraer_precio_descuento(self, driver):
        """Extrae precio con descuento"""
        selectores = [
            ".product-price",  # Selector específico que encontramos
            ".price", ".precio", ".selling-price", ".current-price",
            "[class*='price']", "[class*='precio']",
            "span[data-testid*='price']", "div[data-testid*='price']"
        ]
        
        return self._buscar_precio(driver, selectores, "")
    
    def _extraer_precio_normal(self, driver, precio_desc):
        """Extrae precio normal"""
        selectores = [
            ".original-price", ".old-price", ".list-price",
            ".price--old", ".price-before", ".was-price",
            "[class*='original']", "[class*='old']"
        ]
        
        precio_normal = self._buscar_precio(driver, selectores, "")
        return precio_normal if precio_normal else precio_desc
    
    def _extraer_precio_unidad(self, driver):
        """Extrae precio por unidad - mantiene tu lógica actual"""
        precio_completo = ""
        unidad_text = ""
        
        try:
            selectores_unidad = [
                ".price-per-unit", ".unit-price", ".price-by-weight",
                ".weight-price", "[class*='unit']", "[class*='weight']",
                "[class*='measurement']"
            ]
            
            precio_completo = self._buscar_precio(driver, selectores_unidad, "")
                
        except Exception as e:
            logger.info(f"No se pudo obtener precio por unidad: {e}")
                    
        return precio_completo, unidad_text
    
    def _extraer_descuentos(self, driver):
        """Extrae descuentos con múltiples estrategias"""
        descuentos = []
        
        try:
            # Solo buscar en elementos visibles y con texto relevante
            selectores_descuentos = [
                ".promo-badge", ".discount-badge", ".offer-tag", 
                ".savings", ".promotion", ".discount",
                "[class*='promo']", "[class*='offer']", "[class*='discount']"
            ]
            
            for selector in selectores_descuentos:
                try:
                    elementos = driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elementos:
                        if elem.is_displayed():
                            texto = elem.text.strip()
                            # Filtro más estricto para descuentos reales
                            if (texto and len(texto) <= 50 and  # No textos muy largos
                                any(palabra in texto.upper() for palabra in ['%', 'OFF', 'DCTO', 'DESCUENTO', '2X1', '3X2']) and
                                any(c.isdigit() for c in texto)):  # Debe contener números
                                descuentos.append(texto)
                except:
                    continue

        except Exception as e:
            logger.error(f"Error extrayendo descuentos: {str(e)}")
                
        # Eliminar duplicados
        descuentos_limpios = list(set(descuentos))
        logger.info(f"🎯 Descuentos válidos encontrados: {descuentos_limpios}")
        return descuentos_limpios
    
    def _buscar_precio(self, driver, selectores=None, default=""):
        """Busca precio en múltiples selectores - VERSIÓN UNIFICADA"""
        if selectores is None:
            selectores = [
                ".product-price",  # Selector específico de DeliMart
                ".price", ".precio", ".selling-price", ".current-price",
                "[class*='price']", "[class*='precio']"
            ]
        
        # PRIMERO: Intentar encontrar el contenedor principal del producto
        contenedores_principales = [
            ".col-md-5",  # Contenedor específico del precio según el screenshot
            ".product-detail", 
            "[class*='product']",
            "main", "article"
        ]
        
        elemento_principal = None
        for contenedor in contenedores_principales:
            try:
                elemento_principal = driver.find_element(By.CSS_SELECTOR, contenedor)
                logger.info(f"✅ Contenedor principal encontrado: {contenedor}")
                break
            except:
                continue
        
        # BUSCAR PRECIO en el contenedor principal si lo encontramos
        if elemento_principal:
            for selector in selectores:
                try:
                    elementos = elemento_principal.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elementos:
                        if elem.is_displayed():
                            texto = elem.text.strip()
                            if self._es_precio_valido(texto):
                                logger.info(f"✅ Precio principal encontrado: '{texto}'")
                                return texto
                except Exception as e:
                    logger.debug(f"Selector '{selector}' falló en contenedor principal: {e}")
        
        # SEGUNDO: Si no encontramos en el contenedor principal, buscar en toda la página
        logger.info("🔍 Buscando precio en toda la página...")
        precios_encontrados = []
        
        for selector in selectores:
            try:
                elementos = driver.find_elements(By.CSS_SELECTOR, selector)
                for elem in elementos:
                    if elem.is_displayed():
                        texto = elem.text.strip()
                        if self._es_precio_valido(texto):
                            # Obtener más contexto para determinar si es el precio principal
                            contexto = self._obtener_contexto_precio(elem)
                            precios_encontrados.append({
                                'texto': texto,
                                'selector': selector,
                                'contexto': contexto,
                                'elemento': elem
                            })
                            logger.info(f"💰 Precio encontrado: '{texto}' (contexto: {contexto})")
            except Exception as e:
                logger.debug(f"Selector '{selector}' falló: {e}")
                continue
        
        # Seleccionar el precio más probable
        if precios_encontrados:
            precio_correcto = self._seleccionar_precio_correcto(precios_encontrados)
            return precio_correcto
        
        logger.warning(f"❌ No se encontró precio con {len(selectores)} selectores")
        return default

    def _es_precio_valido(self, texto):
        """Determina si un texto es un precio válido"""
        if not texto:
            return False
        
        # Verificar características de un precio válido
        tiene_simbolo_monetario = '$' in texto or 'USD' in texto
        tiene_digitos = any(c.isdigit() for c in texto)
        longitud_razonable = 2 <= len(texto) <= 20  # Precios no son textos muy largos
        no_es_solo_texto = not texto.replace('$', '').replace(',', '').replace('.', '').replace(' ', '').isalpha()
        
        es_valido = (tiene_simbolo_monetario and 
                    tiene_digitos and 
                    longitud_razonable and 
                    no_es_solo_texto)
        
        return es_valido

    def _seleccionar_precio_correcto(self, precios_encontrados):
        """Selecciona el precio más probable siendo el del producto principal"""
        precios_principales = [p for p in precios_encontrados if p['contexto'] == 'producto_principal']
        
        if precios_principales:
            # Si encontramos precios marcados como principales, tomar el primero
            precio_elegido = precios_principales[0]['texto']
            logger.info(f"🎯 Precio principal seleccionado: '{precio_elegido}'")
            return precio_elegido
        
        # Si no hay precios marcados como principales, usar heurísticas
        precios_ordenados = sorted(precios_encontrados, 
                                key=lambda x: (
                                    0 if 'product-price' in x['selector'] else 1,  # Selector específico primero
                                    0 if 'col-md-5' in str(x['elemento'].get_attribute('outerHTML')) else 1,  # Contenedor correcto
                                    len(x['texto']),  # Más corto primero (generalmente el correcto)
                                ))
        
        precio_elegido = precios_ordenados[0]['texto']
        logger.info(f"🎯 Precio seleccionado por heurísticas: '{precio_elegido}' de {len(precios_encontrados)} opciones")
        
        return precio_elegido
    
    def _obtener_contexto_precio(self, elemento):
        """Obtiene el contexto del precio para determinar si es el principal"""
        try:
            # Subir en el DOM para ver el contexto
            padre = elemento.find_element(By.XPATH, "./..")
            abuelo = padre.find_element(By.XPATH, "./..")
            
            # Verificar si está en secciones de productos relacionados
            contexto_html = abuelo.get_attribute('outerHTML').lower()
            
            if any(palabra in contexto_html for palabra in ['relacionado', 'recomendado', 'visto', 'frecuente', 'juntos']):
                return "producto_secundario"
            elif 'product-detail' in contexto_html or 'col-md-5' in contexto_html:
                return "producto_principal"
            else:
                return "desconocido"
                
        except:
            return "desconocido"



    def _limpiar_precio(self, texto_precio):
        """Limpia y formatea el precio"""
        if not texto_precio or texto_precio == "0":
            return ""
        
        try:
            # Remover caracteres no numéricos excepto punto y coma
            precio_limpio = re.sub(r'[^\d,.]', '', str(texto_precio))
            # Reemplazar coma por punto para decimales
            precio_limpio = precio_limpio.replace(',', '.')
            return precio_limpio
        except:
            return texto_precio