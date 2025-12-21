"""
Módulo de Extracción para Canasta Básica
Responsabilidad: Leer datos de Google Sheets e extraer productos de supermercados
OPTIMIZADO: Procesamiento paralelo, manejo centralizado de cookies, caché
"""
import os
import sys
import pandas as pd
import logging
import time
from datetime import datetime
from typing import Dict, List
from dotenv import load_dotenv

# Agregar directorio padre al path para imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.utils_sheets import ConexionGoogleSheets
from utils.cookie_manager import CookieManager
from utils.optimization import ParallelProcessor, SmartWait, ResultCache, timeit
from extractors.carrefour_extractor import CarrefourExtractor
from extractors.delimart_extractor import DelimartExtractor
from extractors.masonline_extractor import MasonlineExtractor
from extractors.depot_extractor import DepotExtractor
from extractors.lareina_extractor import LareinaExtractor
# from extractors.dia_extractor import DiaExtractor  # Excluido de la ejecución
import re

logger = logging.getLogger(__name__)

class ExtractCanastaBasica:
    """Clase para manejar la extracción de datos de Canasta Básica - OPTIMIZADA"""
    
    def __init__(self, enable_parallel: bool = True, max_workers: int = 3):
        """
        Inicializa el extractor con optimizaciones
        
        Args:
            enable_parallel: Habilitar procesamiento paralelo
            max_workers: Número máximo de workers paralelos
        """
        load_dotenv()
        self.extractors = {}
        self.enable_parallel = enable_parallel
        self.max_workers = max_workers
        
        # Inicializar gestor de cookies centralizado
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.cookie_manager = CookieManager(base_dir)
        self.cookie_manager.migrate_old_cookies()  # Migrar cookies antiguas
        
        # Inicializar caché de resultados
        cache_dir = os.path.join(base_dir, 'cache')
        self.result_cache = ResultCache(cache_dir)
        
        # Inicializar procesador paralelo
        if self.enable_parallel:
            self.parallel_processor = ParallelProcessor(max_workers=max_workers)
        else:
            self.parallel_processor = None
        
        self._setup_extractors()
    
    def _setup_extractors(self):
        """Configura los extractores de supermercados"""
        self.extractors = {
            'carrefour': CarrefourExtractor(),
            'delimart': DelimartExtractor(),
            'masonline': MasonlineExtractor(),
            'depot': DepotExtractor(), 
            'lareina': LareinaExtractor(),
            #'dia' : DiaExtractor()  # Excluido de la ejecución
        }
        logger.info("[OK] Extractores inicializados: %s", list(self.extractors.keys()))
    
    def read_links_from_sheets(self) -> Dict[str, Dict[str, List[Dict]]]:
        """Lee todos los links desde Google Sheets organizados por supermercado"""
        try:
            sheet_id = '13vz5WzXnXLdp61YVHkKO17C4OBEXVJcC5cP38N--8XA'
            gs = ConexionGoogleSheets(sheet_id)
            
            # Obtener nombres de todas las hojas (supermercados)
            sheet_names = self._get_sheet_names(gs)
            logger.info("[OK] Hojas encontradas en el spreadsheet: %s", sheet_names)
            
            all_products_links = {}
            
            for sheet_name in sheet_names:
                logger.info("[EXTRACT] Leyendo hoja: %s", sheet_name)
                
                # Leer datos de la hoja específica
                range_name = f"'{sheet_name}'!A2:D1000"
                df_sheet = gs.leer_df(range_name, header=False)
                
                # Parsear productos y links de esta hoja
                products_links = self._parse_sheet_data(df_sheet, sheet_name)
                
                if products_links:
                    all_products_links[sheet_name] = products_links
                    logger.info("[OK] Hoja %s: %d productos con links", sheet_name, len(products_links))
                else:
                    logger.warning("[WARNING] No se encontraron productos en la hoja %s", sheet_name)
            
            return all_products_links
                
        except Exception as e:
            logger.error("[ERROR] Error leyendo links desde Google Sheets: %s", str(e))
            return {}
    
    def _get_sheet_names(self, gs_connection) -> List[str]:
        """Obtiene los nombres de todas las hojas del spreadsheet"""
        try:
            # Lista de hojas conocidas (excluyendo 'dia')
            known_sheets = ['carrefour', 'delimart', 'masonline', 'depot', 'lareina']
            return known_sheets
            
        except Exception as e:
            logger.warning("[WARNING] No se pudieron obtener nombres de hojas, usando lista por defecto: %s", str(e))
            return ['carrefour', 'delimart', 'masonline', 'depot', 'lareina']
    
    def _parse_sheet_data(self, df_sheet: pd.DataFrame, sheet_name: str) -> Dict[str, List[Dict]]:
        """Parsea los datos de una hoja específica"""
        products = {}
        
        logger.info("[EXTRACT] Analizando hoja %s - Dimensiones: %s", sheet_name, df_sheet.shape)
        
        for idx, row in df_sheet.iterrows():
            # Verificar que la fila tenga datos y que la primera columna (Producto) no esté vacía
            if len(row) >= 2 and pd.notna(row[0]) and str(row[0]).strip():
                product_name = str(row[0]).strip()
                
                # Verificar si hay link en la columna B (índice 1)
                link_cell = row[1] if len(row) > 1 else None
                peso_cell = row[2] if len(row) > 2 else None
                unidad_cell = row[3] if len(row) > 3 else None
                
                # Procesar el link
                link_data = []
                if (pd.notna(link_cell) and 
                    isinstance(link_cell, str) and 
                    self._is_valid_url(link_cell.strip())):
                    
                    # Crear diccionario con todos los datos del producto
                    product_info = {
                        'url': link_cell.strip(),
                        'peso': self._clean_peso_value(peso_cell) if pd.notna(peso_cell) else '',
                        'unidad': self._clean_unidad_value(unidad_cell) if pd.notna(unidad_cell) else '',
                        'producto_nombre': product_name
                    }
                    
                    link_data.append(product_info)
                    logger.debug("[EXTRACT] Hoja %s - Producto: %s - Link: %s", sheet_name, product_name, link_cell.strip())
                
                if link_data:
                    # Si el producto ya existe, agregar a la lista existente
                    if product_name in products:
                        products[product_name].extend(link_data)
                    else:
                        products[product_name] = link_data
        
        logger.info("[OK] Hoja %s procesada: %d productos encontrados", sheet_name, len(products))
        return products
    
    def _clean_peso_value(self, peso_cell):
        """Limpia y formatea el valor del peso"""
        try:
            if pd.isna(peso_cell):
                return ""
            
            peso_str = str(peso_cell).strip()
            peso_str = re.sub(r'[^\d,.]', '', peso_str)
            peso_str = peso_str.replace(',', '.')
            float(peso_str)  # Validar que sea número
            return peso_str
        except:
            return ""
    
    def _clean_unidad_value(self, unidad_cell):
        """Limpia y formatea el valor de la unidad"""
        try:
            if pd.isna(unidad_cell):
                return ""
            
            unidad_str = str(unidad_cell).strip().upper()
            unidad_map = {
                'KG': 'KG', 'KGS': 'KG',
                'GRAMOS': 'G', 'GR': 'G', 'G': 'G',
                'LITROS': 'L', 'L': 'L', 'ML': 'ML',
                'UNIDADES': 'UN', 'UN': 'UN', 'U': 'UN'
            }
            return unidad_map.get(unidad_str, unidad_str)
        except:
            return ""
    
    def _is_valid_url(self, text: str) -> bool:
        """Verifica si un texto es una URL válida"""
        return (text.startswith('http://') or 
                text.startswith('https://') or
                'carrefour.com.ar' in text or
                'delimart.com.ar' in text or
                'masonline.com.ar' in text or
                'depotexpress.com.ar' in text or
                'lareinacorrientes.com.ar' in text or
                'diaonline.supermercadosdia.com.ar' in text)
    
    def initialize_sessions(self):
        """Inicializa las sesiones de todos los supermercados"""
        logger.info("[EXTRACT] Inicializando sesiones de supermercados...")
        
        for supermarket, extractor in self.extractors.items():
            try:
                if hasattr(extractor, 'asegurar_sesion_activa'):
                    if extractor.asegurar_sesion_activa():
                        logger.info("[OK] Sesión activa para %s", supermarket)
                    else:
                        logger.error("[ERROR] No se pudo establecer sesión para %s", supermarket)
                elif hasattr(extractor, 'ensure_active_session'):
                    if extractor.ensure_active_session():
                        logger.info("[OK] Sesión activa para %s", supermarket)
                    else:
                        logger.error("[ERROR] No se pudo establecer sesión para %s", supermarket)
                else:
                    logger.warning("[WARNING] Extractor %s no tiene método de gestión de sesión", supermarket)
                    
            except Exception as e:
                logger.error("[ERROR] Error inicializando sesión para %s: %s", supermarket, str(e))
    
    def extract(self, all_supermarkets_data: Dict[str, Dict[str, List[Dict]]]) -> Dict[str, pd.DataFrame]:
        """
        Extrae productos de todos los supermercados
        OPTIMIZADO: Procesamiento paralelo opcional
        """
        logger.info("[EXTRACT] Iniciando extracción de productos...")
        start_time = time.time()
        
        # Filtrar solo supermercados con extractores configurados
        valid_supermarkets = {
            sm: data for sm, data in all_supermarkets_data.items()
            if sm in self.extractors
        }
        
        if not valid_supermarkets:
            logger.error("[ERROR] No hay supermercados válidos para procesar")
            return {}
        
        # Procesar en paralelo o secuencial según configuración
        if self.enable_parallel and self.parallel_processor and len(valid_supermarkets) > 1:
            logger.info("[EXTRACT] Procesando %d supermercados en PARALELO (%d workers)", 
                       len(valid_supermarkets), self.max_workers)
            all_results = self._extract_parallel(valid_supermarkets)
        else:
            logger.info("[EXTRACT] Procesando %d supermercados en SECUENCIAL", len(valid_supermarkets))
            all_results = self._extract_sequential(valid_supermarkets)
        
        elapsed_time = time.time() - start_time
        logger.info("[EXTRACT] Extracción completada en %.2f segundos (%.2f minutos)", 
                   elapsed_time, elapsed_time / 60)
        
        return all_results
    
    def _extract_sequential(self, supermarkets_data: Dict) -> Dict[str, pd.DataFrame]:
        """Extracción secuencial (método original)"""
        all_results = {}
        
        for supermarket, products_data in supermarkets_data.items():
            logger.info("[EXTRACT] Procesando %s", supermarket.upper())
            df_result = self._process_supermarket(supermarket, products_data)
            
            if not df_result.empty:
                all_results[supermarket] = df_result
                logger.info("[OK] %s: %d registros extraídos", supermarket.upper(), len(df_result))
            else:
                logger.warning("[WARNING] No se extrajeron datos para %s", supermarket)
        
        return all_results
    
    def _extract_parallel(self, supermarkets_data: Dict) -> Dict[str, pd.DataFrame]:
        """Extracción paralela usando ThreadPoolExecutor"""
        def process_supermarket_wrapper(supermarket, products_data):
            """Wrapper para procesar un supermercado"""
            try:
                logger.info("[EXTRACT] [PARALLEL] Iniciando %s", supermarket.upper())
                df_result = self._process_supermarket(supermarket, products_data)
                logger.info("[EXTRACT] [PARALLEL] Completado %s: %d registros", 
                           supermarket.upper(), len(df_result) if not df_result.empty else 0)
                return df_result
            except Exception as e:
                logger.error("[ERROR] [PARALLEL] Error procesando %s: %s", supermarket, str(e))
                return pd.DataFrame()
        
        # Procesar en paralelo
        results = self.parallel_processor.process_supermarkets(
            supermarkets_data,
            process_supermarket_wrapper
        )
        
        # Filtrar resultados vacíos
        all_results = {
            sm: df for sm, df in results.items()
            if df is not None and not df.empty
        }
        
        return all_results
    
    def _process_supermarket(self, supermarket: str, products_data: Dict[str, List[Dict]]) -> pd.DataFrame:
        """Procesa todos los productos de un supermercado"""
        if supermarket not in self.extractors:
            logger.error("[ERROR] Extractor no disponible para %s", supermarket)
            return pd.DataFrame()
        
        extractor = self.extractors[supermarket]
        all_data = []
        start_time = time.time()
        
        try:
            # Verificar sesión activa
            if not self._check_session_active(extractor, supermarket):
                return pd.DataFrame()
            
            # Procesar cada producto con sus metadatos
            for product_name, product_list in products_data.items():
                product_data = self._process_product(extractor, product_name, product_list, supermarket)
                if not product_data.empty:
                    all_data.append(product_data)
            
            # Consolidar resultados básicos
            if all_data:
                final_df = pd.concat(all_data, ignore_index=True)
                processing_time = time.time() - start_time
                logger.info("[OK] Procesamiento %s completado: %d registros en %.1fs", 
                        supermarket, len(final_df), processing_time)
                return final_df
            else:
                return pd.DataFrame()
            
        except Exception as e:
            logger.error("[ERROR] Error crítico procesando %s: %s", supermarket, str(e))
            self._mark_session_expired(extractor)
            return pd.DataFrame()
    
    def _check_session_active(self, extractor, supermarket: str) -> bool:
        """Verifica si la sesión está activa"""
        if (hasattr(extractor, 'session_active') and 
            not extractor.session_active and
            hasattr(extractor, 'ensure_active_session')):
            
            logger.warning("[WARNING] Sesión perdida para %s, reintentando login...", supermarket)
            return extractor.ensure_active_session()
        
        return True
    
    def _mark_session_expired(self, extractor):
        """Marca la sesión como expirada"""
        if hasattr(extractor, 'session_active'):
            extractor.session_active = False
    
    def _process_product(self, extractor, product_name: str, product_list: List[Dict], supermarket: str) -> pd.DataFrame:
        """
        Procesa un producto individual con sus links
        OPTIMIZADO: Usa caché para evitar re-extracciones
        """
        logger.info("[EXTRACT] Procesando %s en %s", product_name, supermarket)
        
        product_results = []
        
        for product_info in product_list:
            url = product_info['url']
            peso = product_info['peso']
            unidad = product_info['unidad']
            
            try:
                # Verificar caché primero (solo para productos del mismo día)
                cache_key = f"{supermarket}_{url}"
                cached_result = self.result_cache.get(cache_key, max_age_hours=24)
                
                if cached_result:
                    logger.debug("[CACHE] Usando resultado en caché para %s", product_name)
                    product_data = cached_result.copy()
                else:
                    # Extraer datos del producto
                    product_data = extractor.extraer_producto(url)
                    
                    # Guardar en caché si es exitoso
                    if product_data and 'error_type' not in product_data:
                        self.result_cache.set(cache_key, product_data)
                
                if product_data and 'error_type' not in product_data:
                    # Agregar metadatos adicionales
                    product_data.update({
                        'peso': peso,
                        'unidad_medida': unidad,
                        'producto_nombre': product_name,
                        'supermercado': supermarket,
                        'url': url,
                        'fecha_extraccion': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    
                    # Asegurar que todos los campos necesarios estén presentes
                    product_data = self._ensure_required_fields(product_data)
                    product_results.append(product_data)
                    
                    logger.info("[OK] %s extraído correctamente (Peso: %s %s)", 
                            product_name, peso, unidad)
                    
                else:
                    # Manejar errores de extracción
                    error_data = self._create_error_product_data(
                        product_name, supermarket, url, peso, unidad, 
                        product_data.get('error_type', 'ERROR_EXTRACCION') if product_data else 'SIN_DATOS'
                    )
                    product_results.append(error_data)
                    logger.warning("[WARNING] Error extrayendo %s: %s", 
                                product_name, error_data['error_type'])
                    
            except Exception as e:
                # Manejar excepciones durante la extracción
                error_data = self._create_error_product_data(
                    product_name, supermarket, url, peso, unidad, f"EXCEPCION: {str(e)}"
                )
                product_results.append(error_data)
                logger.error("[ERROR] Error procesando %s: %s", product_name, str(e))
        
        # Convertir a DataFrame
        if product_results:
            return pd.DataFrame(product_results)
        else:
            return pd.DataFrame()
    
    def _ensure_required_fields(self, product_data: Dict) -> Dict:
        """Asegura que todos los campos requeridos estén presentes"""
        required_fields = {
            'nombre': '',
            'precio_normal': '',
            'precio_descuento': '',
            'precio_por_unidad': '',
            'unidad': '',
            'descuentos': 'Ninguno',
            'fecha': datetime.today().strftime("%Y-%m-%d"),
            'supermercado': '',
            'url': '',
            'peso': '',
            'unidad_medida': '',
            'producto_nombre': ''
        }
        
        for field, default_value in required_fields.items():
            if field not in product_data or product_data[field] is None:
                product_data[field] = default_value
        
        return product_data
    
    def _create_error_product_data(self, product_name: str, supermarket: str, url: str, 
                                peso: str, unidad: str, error_type: str) -> Dict:
        """Crea un registro de producto para casos de error"""
        return {
            'nombre': product_name,
            'precio_normal': '',
            'precio_descuento': '',
            'precio_por_unidad': '',
            'unidad': unidad,
            'descuentos': 'Ninguno',
            'fecha': datetime.today().strftime("%Y-%m-%d"),
            'supermercado': supermarket,
            'url': url,
            'peso': peso,
            'unidad_medida': unidad,
            'producto_nombre': product_name,
            'error_type': error_type,
            'fecha_extraccion': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def cleanup(self):
        """Limpia recursos y cierra conexiones"""
        logger.info("[EXTRACT] Cerrando todos los drivers y guardando sesiones...")
        
        self._save_sessions()
        
        for name, extractor in self.extractors.items():
            try:
                if hasattr(extractor, 'cleanup_driver'):
                    extractor.cleanup_driver()
                    logger.debug("[EXTRACT] Recursos de %s limpiados", name)
                elif hasattr(extractor, 'driver') and extractor.driver:
                    extractor.driver.quit()
                    logger.debug("[EXTRACT] Driver de %s cerrado", name)
            except Exception as e:
                logger.error("[ERROR] Error limpiando recursos de %s: %s", name, str(e))
    
    def _save_sessions(self):
        """
        Guarda las sesiones de todos los supermercados
        OPTIMIZADO: Usa CookieManager centralizado
        """
        logger.info("[EXTRACT] Guardando sesiones para futuras ejecuciones...")
        
        for supermarket, extractor in self.extractors.items():
            try:
                # Intentar guardar usando el método del extractor
                if hasattr(extractor, 'guardar_sesion'):
                    if extractor.guardar_sesion():
                        logger.debug("[EXTRACT] Sesión de %s guardada por extractor", supermarket)
                
                # Si el extractor tiene driver y cookies, guardarlas también con CookieManager
                if hasattr(extractor, 'driver') and extractor.driver:
                    try:
                        cookies = extractor.driver.get_cookies()
                        if cookies:
                            self.cookie_manager.save_cookies(supermarket, cookies)
                            logger.debug("[EXTRACT] Cookies de %s guardadas con CookieManager", supermarket)
                    except Exception as e:
                        logger.debug("[EXTRACT] No se pudieron guardar cookies de %s: %s", supermarket, str(e))
                        
            except Exception as e:
                logger.error("[ERROR] Error guardando sesión de %s: %s", supermarket, str(e))

