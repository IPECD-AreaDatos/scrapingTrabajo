import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import time
import logging
from typing import Dict, List, Tuple, Optional
import re

# Importar extractores y utilidades
from carrefour_extractor import CarrefourExtractor
from delimart_extractor import DelimartExtractor
from masonline_extractor import MasonlineExtractor
from load import load_canasta_basica_data
from utils_db import ConexionBaseDatos
from utils_sheets import ConexionGoogleSheets

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("canasta_basica")


class CanastaBasicaManager:
    """Gestor principal del proceso de canasta básica"""
    
    def __init__(self):
        load_dotenv()
        self._setup_extractors()
        self._setup_database()
        
    def _setup_extractors(self):
        """Configura los extractores de supermercados"""
        self.extractors = {
            #'carrefour': CarrefourExtractor(),
            #'delimart': DelimartExtractor(),
            'masonline': MasonlineExtractor()
        }
        logger.info("Extractores inicializados: %s", list(self.extractors.keys()))
    
    def _setup_database(self):
        """Configura la conexión a base de datos"""
        try:
            host = os.getenv('HOST_DBB')
            user = os.getenv('USER_DBB')
            password = os.getenv('PASSWORD_DBB')
            database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')
            
            self.db = ConexionBaseDatos(host, user, password, database)
            self.db.connect_db()
            logger.info("Conexión a base de datos establecida")
        except Exception as e:
            logger.error("Error configurando base de datos: %s", str(e))
            self.db = None
    
    def read_links_from_sheets(self) -> Dict[str, Dict[str, List[str]]]:
        """Lee todos los links desde Google Sheets organizados por supermercado"""
        try:
            sheet_id = '13vz5WzXnXLdp61YVHkKO17C4OBEXVJcC5cP38N--8XA'
            gs = ConexionGoogleSheets(sheet_id)
            
            # Obtener nombres de todas las hojas (supermercados)
            sheet_names = self._get_sheet_names(gs)
            logger.info("Hojas encontradas en el spreadsheet: %s", sheet_names)
            
            all_products_links = {}
            
            for sheet_name in sheet_names:
                logger.info("Leyendo hoja: %s", sheet_name)
                
                # Leer datos de la hoja específica
                range_name = f"'{sheet_name}'!A2:D6"
                df_sheet = gs.leer_df(range_name, header=False)
                
                # Parsear productos y links de esta hoja
                products_links = self._parse_sheet_data(df_sheet, sheet_name)
                
                if products_links:
                    all_products_links[sheet_name] = products_links
                    logger.info("Hoja %s: %d productos con links", sheet_name, len(products_links))
                else:
                    logger.warning("No se encontraron productos en la hoja %s", sheet_name)
            
            return all_products_links
                
        except Exception as e:
            logger.error("Error leyendo links desde Google Sheets: %s", str(e))
            return {}
    
    def _get_sheet_names(self, gs_connection) -> List[str]:
        """Obtiene los nombres de todas las hojas del spreadsheet"""
        try:
            # Lista de hojas conocidas - agregar más según necesites
            known_sheets = ['carrefour', 'delimart', 'masonline']
            return known_sheets
            
        except Exception as e:
            logger.warning("No se pudieron obtener nombres de hojas, usando lista por defecto: %s", str(e))
            return ['carrefour', 'delimart', 'masonline']
    
    def _parse_sheet_data(self, df_sheet: pd.DataFrame, sheet_name: str) -> Dict[str, List[str]]:
        """Parsea los datos de una hoja específica con la nueva estructura - VERSIÓN DEBUG"""
        products = {}
        
        logger.info(f" Analizando hoja {sheet_name} - Dimensiones: {df_sheet.shape}")
        
        for idx, row in df_sheet.iterrows():
            # Mostrar las primeras filas para debug
            if idx < 5:
                logger.debug(f"Fila {idx}: {list(row)}")
            
            # Verificar que la fila tenga datos y que la primera columna (Producto) no esté vacía
            if len(row) >= 2 and pd.notna(row[0]) and str(row[0]).strip():
                product_name = str(row[0]).strip()
                
                # Verificar si hay link en la columna B (índice 1)
                link_cell = row[1] if len(row) > 1 else None
                peso_cell = row[2] if len(row) > 2 else None
                unidad_cell = row[3] if len(row) > 3 else None
                
                logger.debug(f"Producto: {product_name} | Link: {link_cell} | Peso: {peso_cell} | Unidad: {unidad_cell}")
                
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
                    logger.info(f" Hoja {sheet_name} - Producto: {product_name} - Link: {link_cell.strip()}")
                
                if link_data:
                    # Si el producto ya existe, agregar a la lista existente
                    if product_name in products:
                        products[product_name].extend(link_data)
                    else:
                        products[product_name] = link_data
                else:
                    logger.debug(f" Hoja {sheet_name} - Producto: {product_name} sin link válido")
        
        logger.info(f" Hoja {sheet_name} procesada: {len(products)} productos encontrados")
        return products
    
    def _clean_peso_value(self, peso_cell):
        """Limpia y formatea el valor del peso"""
        try:
            if pd.isna(peso_cell):
                return ""
            
            peso_str = str(peso_cell).strip()
            
            # Remover espacios y caracteres no deseados
            peso_str = re.sub(r'[^\d,.]', '', peso_str)
            
            # Reemplazar coma por punto para decimales
            peso_str = peso_str.replace(',', '.')
            
            # Validar que sea un número
            float(peso_str)  # Solo para validar
            
            return peso_str
        except:
            logger.debug(f"No se pudo limpiar el valor de peso: {peso_cell}")
            return ""

    def _clean_unidad_value(self, unidad_cell):
        """Limpia y formatea el valor de la unidad"""
        try:
            if pd.isna(unidad_cell):
                return ""
            
            unidad_str = str(unidad_cell).strip().upper()
            
            # Normalizar unidades comunes
            unidad_map = {
                'KG': 'KG',
                'KGS': 'KG',
                'GRAMOS': 'G',
                'GR': 'G', 
                'G': 'G',
                'LITROS': 'L',
                'L': 'L',
                'ML': 'ML',
                'UNIDADES': 'UN',
                'UN': 'UN',
                'U': 'UN'
            }
            
            return unidad_map.get(unidad_str, unidad_str)
        except:
            logger.debug(f"No se pudo limpiar el valor de unidad: {unidad_cell}")
            return ""
    
    def _is_valid_url(self, text: str) -> bool:
        """Verifica si un texto es una URL válida"""
        return (text.startswith('http://') or 
                text.startswith('https://') or
                'carrefour.com.ar' in text or
                'delimart.com.ar' in text or
                'masonline.com.ar' in text)
    
    def _extract_all_links(self, products_links: Dict[str, List[str]]) -> List[str]:
        """Extrae todos los links de un diccionario de productos"""
        all_links = []
        for links in products_links.values():
            all_links.extend(links)
        return all_links
    
    def validate_all_links(self, all_supermarkets_data):
        """Valida todos los links y guarda los resultados para usarlos después"""
        try:
            all_validation_results = {}
            all_problematic_products = []
            
            for supermarket, products_links in all_supermarkets_data.items():
                logger.info("🔍 Validando links de %s...", supermarket)
                
                validation_results = self._validate_supermarket_links(supermarket, products_links)
                all_validation_results[supermarket] = validation_results
                
                # Encontrar productos problemáticos
                problematic = self._find_problematic_products(supermarket, validation_results)
                all_problematic_products.extend(problematic)
            
            # Guardar resultados para usarlos después
            self.validation_results = all_validation_results
            
            # Mostrar resumen
            self._show_validation_summary(all_validation_results, all_problematic_products)
            
            # Si hay productos problemáticos, preguntar si continuar
            if all_problematic_products:
                return self._ask_continue_with_problems(all_problematic_products)
            else:
                return True
                
        except Exception as e:
            logger.error("Error en validación completa de links: %s", str(e))
            return False

    def _validate_supermarket_links(self, supermarket, products_data):
        """Valida los links de un supermercado específico"""
        try:
            extractor = self.extractors[supermarket]
            
            # Verificar si el extractor tiene método de validación
            if hasattr(extractor, 'validar_links_productos'):
                logger.info("Validando links con método específico de %s", supermarket)
                
                # Extraer todas las URLs con metadatos
                all_product_data = self._extract_all_links(products_data)
                urls_only = [item['url'] for item in all_product_data]
                
                validation_results = extractor.validar_links_productos(urls_only)
                
                # Reorganizar resultados por producto incluyendo metadatos
                return self._organize_results_by_product_new(products_data, validation_results)
            
            else:
                # Para supermercados sin validación específica
                logger.info("Validando links con método genérico para %s", supermarket)
                return self._generic_link_validation(extractor, products_data)
                
        except Exception as e:
            logger.error("Error validando links de %s: %s", supermarket, str(e))
            return {}
        
    def _organize_results_by_product_new(self, products_data, validation_results):
        """Reorganiza resultados por producto incluyendo metadatos"""
        organized_results = {}
        
        for product_name, product_list in products_data.items():
            product_results = {}
            
            for product_info in product_list:
                url = product_info['url']
                if url in validation_results:
                    # Combinar resultados de validación con metadatos
                    product_results[url] = {
                        **validation_results[url],
                        'peso': product_info['peso'],
                        'unidad': product_info['unidad'],
                        'producto_nombre': product_name
                    }
            
            if product_results:
                organized_results[product_name] = product_results
        
        return organized_results

    def _generic_link_validation(self, extractor, products_data):
        """Validación genérica para supermercados sin método específico"""
        results = {}
        
        for product_name, product_list in products_data.items():
            product_results = {}
            
            for product_info in product_list:
                url = product_info['url']
                peso = product_info['peso']
                unidad = product_info['unidad']
                
                try:
                    # Intentar extraer el producto para ver si funciona
                    data = extractor.extraer_producto(url)
                    
                    if data and 'error_type' not in data:
                        product_results[url] = {
                            'valido': True,
                            'estado': 'OK',
                            'mensaje': 'Link válido - Producto extraíble',
                            'peso': peso,
                            'unidad': unidad,
                            'producto_nombre': product_name
                        }
                    else:
                        product_results[url] = {
                            'valido': False,
                            'estado': data.get('error_type', 'DESCONOCIDO') if data else 'ERROR_EXTRACCION',
                            'mensaje': data.get('titulo', 'Error en extracción') if data else 'No se pudo extraer',
                            'peso': peso,
                            'unidad': unidad,
                            'producto_nombre': product_name
                        }
                        
                except Exception as e:
                    product_results[url] = {
                        'valido': False,
                        'estado': 'ERROR_EXCEPCION',
                        'mensaje': str(e),
                        'peso': peso,
                        'unidad': unidad,
                        'producto_nombre': product_name
                    }
            
            results[product_name] = product_results
        
        return results

    def _organize_results_by_product(self, products_links, validation_results):
        """Reorganiza los resultados de validación por producto"""
        results_by_product = {}
        
        for product_name, urls in products_links.items():
            product_results = {}
            
            for url in urls:
                if url in validation_results:
                    product_results[url] = validation_results[url]
                else:
                    product_results[url] = {
                        'valido': False,
                        'estado': 'NO_VALIDADO',
                        'mensaje': 'URL no fue validada'
                    }
            
            results_by_product[product_name] = product_results
        
        return results_by_product

    def _find_problematic_products(self, supermarket, validation_results):
        """Encuentra productos problemáticos con la nueva estructura"""
        problematic = []
        
        for product_name, links_data in validation_results.items():
            for url, data in links_data.items():
                if not data.get('valido', False):
                    problematic.append({
                        'supermercado': supermarket,
                        'producto': product_name,
                        'url': url,
                        'peso': data.get('peso', ''),
                        'unidad': data.get('unidad', ''),
                        'error': data.get('mensaje', 'Error desconocido')
                    })
        
        return problematic

    def _show_validation_summary(self, all_validation_results, all_problematic_products):
        """Muestra resumen de validación con la nueva estructura"""
        logger.info(" RESUMEN DE VALIDACIÓN DE LINKS")
        logger.info("=" * 50)
        
        total_products = 0
        total_links = 0
        valid_links = 0
        
        for supermarket, products_data in all_validation_results.items():
            supermarket_products = len(products_data)
            supermarket_links = 0
            supermarket_valid = 0
            
            for product_name, links_data in products_data.items():
                supermarket_links += len(links_data)
                supermarket_valid += sum(1 for data in links_data.values() if data.get('valido', False))
            
            total_products += supermarket_products
            total_links += supermarket_links
            valid_links += supermarket_valid
            
            logger.info(" %s: %d productos, %d/%d links válidos", 
                    supermarket.upper(), supermarket_products, supermarket_valid, supermarket_links)
        
        logger.info("=" * 50)
        logger.info(" TOTAL: %d productos, %d/%d links válidos (%.1f%%)", 
                total_products, valid_links, total_links, 
                (valid_links / total_links * 100) if total_links > 0 else 0)
        
        if all_problematic_products:
            logger.info("  PRODUCTOS CON PROBLEMAS:")
            for problem in all_problematic_products:
                logger.info("   - %s: %s (Peso: %s %s) - %s", 
                        problem['supermercado'], problem['producto'],
                        problem['peso'], problem['unidad'], problem['error'])

    def _ask_continue_with_problems(self, problematic_products):
        """Pregunta al usuario si desea continuar con productos problemáticos"""
        logger.warning("  Se encontraron %d productos con menos del 51%% de links válidos", len(problematic_products))
        
        # En modo automático, podemos decidir automáticamente
        # En modo interactivo, podríamos preguntar al usuario
        
        response = input("¿Desea continuar con la extracción? (s/n): ").strip().lower()
        return response in ['s', 'si', 'sí', 'y', 'yes']
    
    def check_links_validity_percentage(self, all_supermarkets_data, min_percentage=51):
        """Verifica el porcentaje de links válidos con la nueva estructura"""
        try:
            total_links = 0
            valid_links = 0
            
            for supermarket, products_data in all_supermarkets_data.items():
                if supermarket in self.validation_results:
                    for product_name, links_data in self.validation_results[supermarket].items():
                        for url, validation_data in links_data.items():
                            total_links += 1
                            if validation_data.get('valido', False):
                                valid_links += 1
            
            if total_links == 0:
                logger.error("No se encontraron links para validar")
                return False
            
            validity_percentage = (valid_links / total_links) * 100
            logger.info("Porcentaje de links válidos: %.1f%% (%d/%d)", 
                    validity_percentage, valid_links, total_links)
            
            if validity_percentage >= min_percentage:
                logger.info(" Porcentaje de links válidos aceptable (>=%d%%)", min_percentage)
                return True
            else:
                logger.error(" Porcentaje de links válidos insuficiente (%.1f%% < %d%%)", 
                            validity_percentage, min_percentage)
                return False
                
        except Exception as e:
            logger.error("Error calculando porcentaje de links válidos: %s", str(e))
            return False
    
    def initialize_sessions(self):
        """Inicializa las sesiones de todos los supermercados"""
        logger.info("Inicializando sesiones de supermercados...")
        
        for supermarket, extractor in self.extractors.items():
            try:
                # USAR LOS MÉTODOS CORRECTOS
                if hasattr(extractor, 'asegurar_sesion_activa'):
                    if extractor.asegurar_sesion_activa():
                        logger.info("Sesión activa para %s", supermarket)
                    else:
                        logger.error("No se pudo establecer sesión para %s", supermarket)
                elif hasattr(extractor, 'ensure_active_session'):
                    if extractor.ensure_active_session():
                        logger.info("Sesión activa para %s", supermarket)
                    else:
                        logger.error("No se pudo establecer sesión para %s", supermarket)
                else:
                    logger.warning("Extractor %s no tiene método de gestión de sesión", supermarket)
                    
            except Exception as e:
                logger.error("Error inicializando sesión para %s: %s", supermarket, str(e))
    
    def process_supermarket(self, supermarket: str, products_data: Dict[str, List[str]]) -> pd.DataFrame:
        """Procesa todos los productos de un supermercado"""
        if supermarket not in self.extractors:
            logger.error("Extractor no disponible para %s", supermarket)
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
            
            # Consolidar resultados
            final_df = self._consolidate_results(all_data)
            processing_time = time.time() - start_time
            
            logger.info("Procesamiento %s completado: %d registros en %.1fs", 
                    supermarket, len(final_df), processing_time)
            
            return final_df
            
        except Exception as e:
            logger.error("Error crítico procesando %s: %s", supermarket, str(e))
            self._mark_session_expired(extractor)
            return pd.DataFrame()
    
    def _check_session_active(self, extractor, supermarket: str) -> bool:
        """Verifica si la sesión está activa"""
        if (hasattr(extractor, 'session_active') and 
            not extractor.session_active and
            hasattr(extractor, 'ensure_active_session')):
            
            logger.warning("Sesión perdida para %s, reintentando login...", supermarket)
            return extractor.ensure_active_session()
        
        return True
    
    def _mark_session_expired(self, extractor):
        """Marca la sesión como expirada"""
        if hasattr(extractor, 'session_active'):
            extractor.session_active = False
    
    def _process_product(self, extractor, product_name: str, product_list: List[str], supermarket: str) -> pd.DataFrame:
        """Procesa un producto individual con sus links"""
        logger.info("Procesando %s en %s", product_name, supermarket)
        
        product_results = []
    
        for product_info in product_list:
            url = product_info['url']
            peso = product_info['peso']
            unidad = product_info['unidad']
            
            try:
                logger.info("Extrayendo: %s", product_name)
                
                # Extraer datos del producto
                product_data = extractor.extraer_producto(url)
                
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
                    
                    logger.info(" %s extraído correctamente (Peso: %s %s)", 
                            product_name, peso, unidad)
                    
                else:
                    # Manejar errores de extracción
                    error_data = self._create_error_product_data(
                        product_name, supermarket, url, peso, unidad, 
                        product_data.get('error_type', 'ERROR_EXTRACCION') if product_data else 'SIN_DATOS'
                    )
                    product_results.append(error_data)
                    logger.warning(" Error extrayendo %s: %s", 
                                product_name, error_data['error_type'])
                    
            except Exception as e:
                # Manejar excepciones durante la extracción
                error_data = self._create_error_product_data(
                    product_name, supermarket, url, peso, unidad, f"EXCEPCION: {str(e)}"
                )
                product_results.append(error_data)
                logger.error(" Error procesando %s: %s", product_name, str(e))
        
        # Convertir a DataFrame
        if product_results:
            return pd.DataFrame(product_results)
        else:
            return pd.DataFrame()
        
    def _ensure_required_fields(self, product_data: Dict) -> Dict:
        """Asegura que todos los campos requeridos estén presentes en los datos del producto"""
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
        
        # Combinar con valores por defecto
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
    
    def _consolidate_results(self, all_data: List[pd.DataFrame]) -> pd.DataFrame:
        """Consolida múltiples DataFrames en uno solo"""
        if not all_data:
            return pd.DataFrame()
        
        # Concatenar todos los DataFrames
        consolidated_df = pd.concat(all_data, ignore_index=True)
        
        # Ordenar por producto y supermercado
        if 'producto_nombre' in consolidated_df.columns and 'supermercado' in consolidated_df.columns:
            consolidated_df = consolidated_df.sort_values(['producto_nombre', 'supermercado'])
        
        # Reordenar columnas para mejor presentación
        column_order = [
            'producto_nombre', 'nombre', 'supermercado', 
            'precio_normal', 'precio_descuento', 'precio_por_unidad',
            'unidad', 'unidad_medida', 'peso', 'descuentos',
            'fecha', 'fecha_extraccion', 'url'
        ]
        
        # Agregar columnas de error si existen
        if 'error_type' in consolidated_df.columns:
            column_order.append('error_type')
        
        # Reordenar columnas existentes
        existing_columns = [col for col in column_order if col in consolidated_df.columns]
        other_columns = [col for col in consolidated_df.columns if col not in column_order]
        
        consolidated_df = consolidated_df[existing_columns + other_columns]
        
        return consolidated_df
    
    def save_results(self, df: pd.DataFrame, supermarket: str):
        """Guarda los resultados en CSV y base de datos"""
        if df.empty:
            logger.warning("No hay datos para guardar de %s", supermarket)
            return
        
        # Guardar CSV
        csv_file = f'productos_{supermarket}_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
        df.to_csv(csv_file, index=False, encoding='utf-8')
        logger.info("Datos guardados en %s", csv_file)
        
        # Cargar a base de datos
        if self.db:
            try:
                new_data = load_canasta_basica_data(df)
                if new_data:
                    logger.info("Carga a base de datos completada: %d registros", len(df))
                else:
                    logger.warning("No se cargaron datos nuevos a la base de datos")
            except Exception as e:
                logger.error("Error cargando a base de datos: %s", str(e))
        else:
            logger.warning("No hay conexión a base de datos para guardar resultados")
    
    def save_sessions(self):
        """Guarda las sesiones de todos los supermercados"""
        logger.info("Guardando sesiones para futuras ejecuciones...")
        
        for supermarket, extractor in self.extractors.items():
            try:
                if hasattr(extractor, 'save_session'):
                    if extractor.guardar_sesion():
                        logger.debug("Sesión de %s guardada", supermarket)
                    else:
                        logger.warning("No se pudo guardar sesión de %s", supermarket)
            except Exception as e:
                logger.error("Error guardando sesión de %s: %s", supermarket, str(e))
    
    def cleanup(self):
        """Limpia recursos y cierra conexiones"""
        logger.info("Cerrando todos los drivers y guardando sesiones...")
        
        self.save_sessions()
        
        for name, extractor in self.extractors.items():
            try:
                if hasattr(extractor, 'cleanup'):
                    extractor.cleanup_driver()
                    logger.debug("Recursos de %s limpiados", name)
                elif hasattr(extractor, 'driver') and extractor.driver:
                    extractor.driver.quit()
                    logger.debug("Driver de %s cerrado", name)
                else:
                    logger.debug("No hay recursos activos para %s", name)
            except Exception as e:
                logger.error("Error limpiando recursos de %s: %s", name, str(e))
    
    def run_etl_process(self):
        """Ejecuta el proceso ETL completo"""
        logger.info("=" * 80)
        logger.info("Iniciando proceso ETL para Canasta Básica")
        logger.info("=" * 80)
        
        try:
            # 1. Leer datos de entrada (organizados por supermercado)
            all_supermarkets_data = self.read_links_from_sheets()
            if not all_supermarkets_data:
                logger.error("No se encontraron datos en Google Sheets")
                return
            
            logger.info("Se encontraron datos para %d supermercados: %s", 
                       len(all_supermarkets_data), list(all_supermarkets_data.keys()))
            
            # Mostrar estadísticas por supermercado con nueva estructura
            for supermarket, products_data in all_supermarkets_data.items():
                total_products = len(products_data)
                total_links = sum(len(product_list) for product_list in products_data.values())
                
                # Calcular productos con metadatos completos
                products_with_metadata = 0
                for product_list in products_data.values():
                    for product_info in product_list:
                        if product_info.get('peso') and product_info.get('unidad'):
                            products_with_metadata += 1
                
                logger.info("Supermercado %s: %d productos, %d links, %d con metadatos completos", 
                        supermarket, total_products, total_links, products_with_metadata)
                
            # 2. NUEVO: Validación completa de links
            logger.info("=== FASE 1: VALIDACIÓN DE LINKS ===")
            should_continue = self.validate_all_links(all_supermarkets_data)
            
            if not should_continue:
                logger.info("Proceso cancelado por el usuario")
                return
            
            # 2.2 VERIFICACIÓN DE PORCENTAJE DE LINKS VÁLIDOS
            logger.info("=== FASE 2: VERIFICACIÓN DE PORCENTAJES ===")
            if not self.check_links_validity_percentage(all_supermarkets_data, min_percentage=51):
                logger.error(" Proceso cancelado por bajo porcentaje de links válidos")
                #exit()

            logger.info("Validación exitosa - Continuando con extracción...")
            
            # 3. Inicializar sesiones
            self.initialize_sessions()
            
            # 4. Procesar cada supermercado con SUS datos específicos (nueva estructura)
            all_results = []
            
            for supermarket, products_data in all_supermarkets_data.items():
                if supermarket not in self.extractors:
                    logger.warning("No hay extractor configurado para %s, saltando...", supermarket)
                    continue
                    
                logger.info("=" * 50)
                logger.info("Procesando %s", supermarket.upper())
                logger.info("=" * 50)
                
                # Usar la nueva versión que procesa la estructura con metadatos
                df_result = self.process_supermarket(supermarket, products_data)
                
                if not df_result.empty:
                    all_results.append(df_result)
                    self.save_results(df_result, supermarket)
                    
                    # Mostrar resumen del procesamiento
                    valid_products = len(df_result[df_result['precio_normal'].notna() & (df_result['precio_normal'] != '')])
                    total_products = len(df_result)
                    logger.info("%s: %d/%d productos extraídos exitosamente", 
                            supermarket.upper(), valid_products, total_products)
                else:
                    logger.warning("No se extrajeron datos para %s", supermarket)
            
            # 5. Consolidar resultados finales
            if all_results:
                final_df = pd.concat(all_results, ignore_index=True)
                # ELIMINAR COLUMNA 'unidad' (duplicada con 'unidad_medida')
                if 'unidad' in final_df.columns and 'unidad_medida' in final_df.columns:
                    logger.info("Eliminando columna duplicada 'unidad', conservando 'unidad_medida'")
                    final_df = final_df.drop(columns=['unidad'])
                final_csv = f'canasta_basica_completa_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
                final_df.to_csv(final_csv, index=False, encoding='utf-8')
                logger.info("Resultados consolidados guardados en %s", final_csv)
                logger.info("Total de registros extraídos: %d", len(final_df))
                
                # Mostrar resumen por supermercado
                supermarket_counts = final_df['supermercado'].value_counts()
                logger.info("Resumen por supermercado: %s", supermarket_counts.to_dict())
                
                # Mostrar resumen de productos con precios
                products_with_prices = final_df[final_df['precio_normal'].notna() & (final_df['precio_normal'] != '')]
                logger.info("Productos con precios extraídos: %d/%d", 
                        len(products_with_prices), len(final_df))
                
                # Mostrar productos sin precios (para debugging)
                products_without_prices = final_df[final_df['precio_normal'].isna() | (final_df['precio_normal'] == '')]
                if not products_without_prices.empty:
                    logger.warning("Productos sin precios extraídos:")
                    for idx, row in products_without_prices.iterrows():
                        logger.warning("  - %s (%s)", row['producto_nombre'], row['supermercado'])
            else:
                logger.warning("No se extrajeron datos de ningún supermercado")
            
            logger.info("Proceso ETL completado exitosamente")
            
        except Exception as e:
            logger.error("Error en proceso ETL: %s", str(e))
            raise
        
        finally:
            self.cleanup()


def run_canasta_basica():
    """Función principal para ejecutar el proceso de canasta básica"""
    manager = CanastaBasicaManager()
    manager.run_etl_process()


if __name__ == "__main__":
    """Ejecutar cuando el script se llama directamente"""
    print("Iniciando proceso de Canasta Básica...")
    run_canasta_basica()