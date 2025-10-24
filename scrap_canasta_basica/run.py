import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
import os
import time
import logging
from typing import Dict, List, Tuple, Optional

# Importar extractores y utilidades
from carrefour_extractor import CarrefourExtractor
from delimart_extractor import DelimartExtractor
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
            'carrefour': CarrefourExtractor(),
            'delimart': DelimartExtractor()
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
                range_name = f"'{sheet_name}'!A2:AI2"
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
            known_sheets = ['carrefour', 'delimart']
            return known_sheets
            
        except Exception as e:
            logger.warning("No se pudieron obtener nombres de hojas, usando lista por defecto: %s", str(e))
            return ['carrefour', 'delimart']
    
    def _parse_sheet_data(self, df_sheet: pd.DataFrame, sheet_name: str) -> Dict[str, List[str]]:
        """Parsea los datos de una hoja específica"""
        products = {}
        
        for idx, row in df_sheet.iterrows():
            # Verificar que la fila tenga datos y que la primera columna no esté vacía
            if len(row) > 0 and pd.notna(row[0]) and str(row[0]).strip():
                product_name = str(row[0]).strip()
                links = []
                
                # Extraer links de las columnas B-K (índices 1-10)
                for i in range(1, len(row)):
                    cell_value = row[i]
                    if (pd.notna(cell_value) and 
                        isinstance(cell_value, str) and 
                        self._is_valid_url(cell_value.strip())):
                        
                        links.append(cell_value.strip())
                
                if links:
                    products[product_name] = links
                    logger.debug("Hoja %s - Producto: %s - %d links", 
                               sheet_name, product_name, len(links))
                else:
                    logger.debug("Hoja %s - Producto: %s sin links válidos", 
                               sheet_name, product_name)
        
        return products
    
    def _is_valid_url(self, text: str) -> bool:
        """Verifica si un texto es una URL válida"""
        return (text.startswith('http://') or 
                text.startswith('https://') or
                'carrefour.com.ar' in text or
                'delimart.com.ar' in text)
    
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

    def _validate_supermarket_links(self, supermarket, products_links):
        """Valida los links de un supermercado específico"""
        try:
            extractor = self.extractors[supermarket]
            
            # Verificar si el extractor tiene método de validación
            if hasattr(extractor, 'validar_links_productos'):
                logger.info("Validando links con método específico de %s", supermarket)
                
                # Extraer todas las URLs
                all_urls = self._extract_all_links(products_links)
                validation_results = extractor.validar_links_productos(all_urls)
                
                # Reorganizar resultados por producto
                return self._organize_results_by_product(products_links, validation_results)
            
            else:
                # Para supermercados sin validación específica (como Delimart)
                logger.info("Validando links con método genérico para %s", supermarket)
                return self._generic_link_validation(extractor, products_links)
                
        except Exception as e:
            logger.error("Error validando links de %s: %s", supermarket, str(e))
            return {}

    def _generic_link_validation(self, extractor, products_links):
        """Validación genérica para supermercados sin método específico"""
        results = {}
        
        for product_name, urls in products_links.items():
            product_results = {}
            
            for url in urls:
                try:
                    # Intentar extraer el producto para ver si funciona
                    data = extractor.extraer_producto(url)
                    
                    if data and 'error_type' not in data:
                        product_results[url] = {
                            'valido': True,
                            'estado': 'OK',
                            'mensaje': 'Link válido - Producto extraíble'
                        }
                    else:
                        product_results[url] = {
                            'valido': False,
                            'estado': data.get('error_type', 'DESCONOCIDO') if data else 'ERROR_EXTRACCION',
                            'mensaje': data.get('titulo', 'Error en extracción') if data else 'No se pudo extraer'
                        }
                        
                except Exception as e:
                    product_results[url] = {
                        'valido': False,
                        'estado': 'ERROR_EXCEPCION',
                        'mensaje': str(e)
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
        """Encuentra productos con menos del 51% de links válidos"""
        problematic = []
        
        for product_name, product_results in validation_results.items():
            total_links = len(product_results)
            valid_links = sum(1 for result in product_results.values() if result.get('valido', False))
            
            percentage = (valid_links / total_links) * 100 if total_links > 0 else 0
            
            if percentage < 51:
                problematic.append({
                    'supermercado': supermarket,
                    'producto': product_name,
                    'links_validos': valid_links,
                    'links_totales': total_links,
                    'porcentaje': percentage,
                    'links_invalidos': [
                        url for url, result in product_results.items() 
                        if not result.get('valido', False)
                    ]
                })
        
        return problematic

    def _show_validation_summary(self, validation_results, problematic_products):
        """Muestra resumen completo de la validación POR PRODUCTO"""
        logger.info("📊 ========== RESUMEN DE VALIDACIÓN COMPLETA ==========")
        
        for supermarket, results in validation_results.items():
            logger.info("🏪 %s:", supermarket.upper())
            
            # Mostrar POR PRODUCTO
            for product_name, product_results in results.items():
                total_links = len(product_results)
                valid_links = sum(1 for result in product_results.values() if result.get('valido', False))
                percentage = (valid_links / total_links) * 100 if total_links > 0 else 0
                
                status_icon = "✅" if percentage >= 51 else "❌"
                
                logger.info("   %s %s: %d/%d links válidos (%.1f%%)", 
                        status_icon, product_name, valid_links, total_links, percentage)
                
                # Mostrar links inválidos si el producto tiene problemas
                if percentage < 51:
                    invalid_links = [url for url, result in product_results.items() if not result.get('valido', False)]
                    logger.info("      🔗 Links inválidos:")
                    for url in invalid_links:
                        logger.info("         - %s", url)
        
        # Mostrar productos problemáticos (menos del 51%)
        if problematic_products:
            logger.info("🚨 PRODUCTOS CON PROBLEMAS (menos del 51%% de links válidos):")
            for problem in problematic_products:
                logger.info("   ❌ %s - %s: %d/%d links válidos (%.1f%%)", 
                        problem['supermercado'], problem['producto'],
                        problem['links_validos'], problem['links_totales'],
                        problem['porcentaje'])
        else:
            logger.info("🎉 TODOS los productos tienen más del 51%% de links válidos!")
        
        logger.info("======================================================")

    def _ask_continue_with_problems(self, problematic_products):
        """Pregunta al usuario si desea continuar con productos problemáticos"""
        logger.warning("⚠️  Se encontraron %d productos con menos del 51%% de links válidos", len(problematic_products))
        
        # En modo automático, podemos decidir automáticamente
        # En modo interactivo, podríamos preguntar al usuario
        
        response = input("¿Desea continuar con la extracción? (s/n): ").strip().lower()
        return response in ['s', 'si', 'sí', 'y', 'yes']
    
    def check_links_validity_percentage(self, all_supermarkets_data, min_percentage=51):
        """
        Verifica que todos los productos tengan al menos el porcentaje mínimo de links válidos
        Adaptada para la estructura real: {'supermarket': {'producto': [urls]}}
        """
        logger.info("=== VERIFICACIÓN DE PORCENTAJES DE LINKS VÁLIDOS ===")
        low_percentage_products = []
        
        # Primero necesitamos obtener los resultados de la validación
        # Asumo que los tienes almacenados en self.validation_results o similar
        validation_results = getattr(self, 'validation_results', {})
        
        for supermarket_name, products_dict in all_supermarkets_data.items():
            # products_dict es {'producto': [url1, url2, ...]}
            for product_name, url_list in products_dict.items():
                total_links = len(url_list)
                
                if total_links > 0:
                    # Buscar los resultados de validación para este producto
                    valid_links = 0
                    
                    # Buscar en validation_results
                    if (supermarket_name in validation_results and 
                        product_name in validation_results[supermarket_name]):
                        
                        product_results = validation_results[supermarket_name][product_name]
                        valid_links = sum(1 for result in product_results.values() 
                                        if result.get('valido', False))
                    
                    percentage_valid = (valid_links / total_links) * 100
                    
                    logger.info(f"🔍 {supermarket_name} - {product_name}: {valid_links}/{total_links} links válidos ({percentage_valid:.1f}%)")
                    
                    if percentage_valid < min_percentage:
                        low_percentage_products.append({
                            'supermarket': supermarket_name,
                            'product': product_name,
                            'percentage': percentage_valid,
                            'valid': valid_links,
                            'total': total_links
                        })
        
        # Reportar resultados
        if low_percentage_products:
            logger.error(f"🚫 Se detectaron {len(low_percentage_products)} productos con menos del {min_percentage}% de links válidos:")
            for item in low_percentage_products:
                logger.error(f"   - {item['product']} en {item['supermarket']}: {item['percentage']:.1f}% ({item['valid']}/{item['total']})")
            return False
        else:
            logger.info(f"✅ Todos los productos tienen al menos {min_percentage}% de links válidos")
            return True
    
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
    
    def process_supermarket(self, supermarket: str, products_links: Dict[str, List[str]]) -> pd.DataFrame:
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
            
            # Procesar cada producto
            for product_name, links in products_links.items():
                product_data = self._process_product(extractor, product_name, links, supermarket)
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
    
    def _process_product(self, extractor, product_name: str, links: List[str], supermarket: str) -> pd.DataFrame:
        """Procesa un producto individual con sus links"""
        logger.info("Procesando %s en %s", product_name, supermarket)
        
        processed_data = []
        success_count = 0
        
        for url in links:
            try:
                data = extractor.extract_product(url)
                
                if data and 'error_type' not in data:
                    # Link exitoso
                    data['producto_principal'] = product_name
                    data['supermercado'] = supermarket
                    processed_data.append(data)
                    success_count += 1
                    logger.debug("Producto extraído correctamente: %s", product_name)
                else:
                    logger.warning("No se pudieron extraer datos de %s", url)
                
                # Pausa entre requests
                time.sleep(0.5)
                
            except Exception as e:
                logger.error("Error procesando %s: %s", url, str(e))
                continue
        
        logger.info("Resumen %s: %d/%d exitosos", 
                   product_name, success_count, len(links))
        
        return pd.DataFrame(processed_data) if processed_data else pd.DataFrame()
    
    def _consolidate_results(self, data_frames: List[pd.DataFrame]) -> pd.DataFrame:
        """Consolida múltiples DataFrames en uno solo"""
        if not data_frames:
            return pd.DataFrame()
        
        final_df = pd.concat(data_frames, ignore_index=True)
        
        # Ordenar columnas
        if 'producto_principal' in final_df.columns:
            columns = ['producto_principal', 'supermercado'] + \
                     [col for col in final_df.columns if col not in ['producto_principal', 'supermercado']]
            final_df = final_df[columns]
        
        return final_df
    
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
            
            # Mostrar estadísticas por supermercado
            for supermarket, products_data in all_supermarkets_data.items():
                total_links = sum(len(links) for links in products_data.values())
                logger.info("Supermercado %s: %d productos, %d links totales", 
                           supermarket, len(products_data), total_links)
                
            # 2. NUEVO: Validación completa de links
            logger.info("=== FASE 1: VALIDACIÓN DE LINKS ===")
            should_continue = self.validate_all_links(all_supermarkets_data)
            
            if not should_continue:
                logger.info("⏹️  Proceso cancelado por el usuario")
                return
            
            # 2.2 VERIFICACIÓN DE PORCENTAJE DE LINKS VÁLIDOS
            logger.info("=== FASE 2: VERIFICACIÓN DE PORCENTAJES ===")
            if not self.check_links_validity_percentage(all_supermarkets_data, min_percentage=51):
                logger.error(" Proceso cancelado por bajo porcentaje de links válidos")
                exit()

            logger.info("✅ Validación exitosa - Continuando con extracción...")
            
            # 3. Inicializar sesiones
            self.initialize_sessions()
            
            # 4. Procesar cada supermercado con SUS links específicos
            all_results = []
            
            for supermarket, products_links in all_supermarkets_data.items():
                if supermarket not in self.extractors:
                    logger.warning("No hay extractor configurado para %s, saltando...", supermarket)
                    continue
                    
                logger.info("=" * 50)
                logger.info("Procesando %s", supermarket.upper())
                logger.info("=" * 50)
                
                df_result = self.process_supermarket(supermarket, products_links)
                
                if not df_result.empty:
                    all_results.append(df_result)
                    self.save_results(df_result, supermarket)
                else:
                    logger.warning("No se extrajeron datos para %s", supermarket)
            
            # 5. Consolidar resultados finales
            if all_results:
                final_df = pd.concat(all_results, ignore_index=True)
                final_csv = f'canasta_basica_completa_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
                final_df.to_csv(final_csv, index=False, encoding='utf-8')
                logger.info("Resultados consolidados guardados en %s", final_csv)
                logger.info("Total de registros extraídos: %d", len(final_df))
                
                # Mostrar resumen por supermercado
                supermarket_counts = final_df['supermercado'].value_counts()
                logger.info("Resumen por supermercado: %s", supermarket_counts.to_dict())
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