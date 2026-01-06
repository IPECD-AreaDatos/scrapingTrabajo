"""
Módulo de Extracción para Canasta Básica
Responsabilidad: Leer datos de Google Sheets e extraer productos de supermercados
OPTIMIZADO: Arquitectura Producer-Consumer (Cola Global) para balanceo de carga
"""
import os
import sys
import pandas as pd
import logging
import time
import threading
import queue
import re
from datetime import datetime
from typing import Dict, List, Any
from dotenv import load_dotenv

# Agregar directorio padre al path para imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.utils_sheets import ConexionGoogleSheets
from utils.cookie_manager import CookieManager
from utils.optimization import ResultCache
from extractors.carrefour_extractor import CarrefourExtractor
from extractors.delimart_extractor import DelimartExtractor
from extractors.masonline_extractor import MasonlineExtractor
from extractors.depot_extractor import DepotExtractor
from extractors.lareina_extractor import LareinaExtractor
from extractors.dia_extractor import DiaExtractor
from extractors.paradacanga_extractor import ParadacangaExtractor

logger = logging.getLogger(__name__)

class ExtractCanastaBasica:
    """Clase para manejar la extracción de datos de Canasta Básica - ARQUITECTURA COLA GLOBAL"""
    
    def __init__(self, enable_parallel: bool = True, max_workers: int = 3):
        """
        Inicializa el extractor
        Args:
            enable_parallel: Habilitar procesamiento paralelo
            max_workers: Número de hilos simultáneos (Navegadores activos a la vez)
        """
        load_dotenv()
        self.enable_parallel = enable_parallel
        # Si no es paralelo, forzamos 1 worker
        self.max_workers = max_workers if enable_parallel else 1
        
        # Mapeo de clases de extractores para instanciación dinámica
        self.extractor_classes = {
            'carrefour': CarrefourExtractor,
            'delimart': DelimartExtractor,
            'masonline': MasonlineExtractor,
            'depot': DepotExtractor, 
            'lareina': LareinaExtractor,
            'dia' : DiaExtractor, 
            'paradacanga' : ParadacangaExtractor()
        }

        # Inicializar gestor de cookies
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.cookie_manager = CookieManager(base_dir)
        self.cookie_manager.migrate_old_cookies()
        
        # Inicializar caché
        cache_dir = os.path.join(base_dir, 'cache')
        self.result_cache = ResultCache(cache_dir)
        
    def initialize_sessions(self):
        """
        Compatibilidad con main.py:
        En esta arquitectura optimizada, las sesiones se inician dinámicamente
        dentro de cada worker, no al principio.
        """
        logger.info("[EXTRACT] Modo Paralelo Optimizado: Las sesiones se iniciarán bajo demanda en los workers.")
        pass

    def cleanup(self):
        """
        Compatibilidad con main.py:
        Los workers limpian sus propios recursos (drivers) al terminar su cola de tareas.
        """
        logger.info("[EXTRACT] Cleanup: Recursos ya liberados automáticamente por los workers.")
        pass

    def read_links_from_sheets(self) -> Dict[str, Dict[str, List[Dict]]]:
        """Lee todos los links desde Google Sheets (Lógica Original)"""
        try:
            sheet_id = '13vz5WzXnXLdp61YVHkKO17C4OBEXVJcC5cP38N--8XA'
            gs = ConexionGoogleSheets(sheet_id)
            sheet_names = ['carrefour', 'delimart', 'masonline', 'depot', 'lareina', 'dia', 'paradacanga']
            all_products_links = {}
            
            logger.info("[EXTRACT] Leyendo hojas del spreadsheet...")
            
            for sheet_name in sheet_names:
                try:
                    # NOTA: Rango fijo según tu código original. 
                    # Si agregas más productos, asegúrate de ampliar 'D716'
                    range_name = f"'{sheet_name}'!A2:D10"
                    df_sheet = gs.leer_df(range_name, header=False)
                    products_links = self._parse_sheet_data(df_sheet, sheet_name)
                    if products_links:
                        all_products_links[sheet_name] = products_links
                except Exception as e:
                    logger.warning(f"No se pudo leer la hoja {sheet_name}: {e}")
            
            return all_products_links
        except Exception as e:
            logger.error(f"[ERROR] Error leyendo Sheets: {e}")
            return {}

    def extract(self, all_supermarkets_data: Dict[str, Dict[str, List[Dict]]]) -> Dict[str, pd.DataFrame]:
        """
        Método principal de extracción usando Cola Global (Producer-Consumer).
        Reemplaza a _extract_parallel y _extract_sequential originales.
        """
        logger.info(f"[EXTRACT] Iniciando extracción con {self.max_workers} workers...")
        start_time = time.time()
        
        # 1. Aplanar todos los productos en una cola de tareas única
        task_queue = queue.Queue()
        total_tasks = 0
        
        # Validar qué supermercados tenemos disponibles en el código
        valid_supers = [s for s in all_supermarkets_data.keys() if s in self.extractor_classes]
        
        for supermarket in valid_supers:
            products_dict = all_supermarkets_data[supermarket]
            for prod_name, variants in products_dict.items():
                for variant in variants:
                    # Crear tarea
                    task = {
                        'supermarket': supermarket,
                        'product_name': prod_name,
                        'url': variant['url'],
                        'peso': variant['peso'],
                        'unidad': variant['unidad']
                    }
                    task_queue.put(task)
                    total_tasks += 1
                    
        logger.info(f"[EXTRACT] Cola generada con {total_tasks} productos totales.")
        
        # 2. Estructura para recolectar resultados de forma segura entre hilos
        results_list = []
        results_lock = threading.Lock()
        
        # 3. Definición del Worker (El motor de la paralelización)
        def worker_loop(worker_id):
            logger.debug(f"[WORKER {worker_id}] Iniciado.")
            
            # Cada worker tiene sus propios extractores para no mezclar sesiones de Selenium
            local_extractors = {} 
            
            while True:
                try:
                    # Intentar obtener tarea sin bloquear indefinidamente
                    task = task_queue.get(block=False)
                except queue.Empty:
                    # Si la cola está vacía, el worker termina
                    break
                
                sm = task['supermarket']
                
                try:
                    # Instanciar extractor si este worker aún no lo tiene
                    if sm not in local_extractors:
                        try:
                            # Instanciamos el extractor específico para este hilo
                            local_extractors[sm] = self.extractor_classes[sm]()
                        except Exception as e:
                            logger.error(f"[WORKER {worker_id}] Falló init driver {sm}: {e}")
                            task_queue.task_done()
                            continue

                    # Procesar la tarea
                    extractor_instance = local_extractors[sm]
                    df_result = self._process_single_task(extractor_instance, task)
                    
                    # Guardar resultado de forma segura
                    if not df_result.empty:
                        with results_lock:
                            results_list.append(df_result)
                            
                except Exception as e:
                    logger.error(f"[WORKER {worker_id}] Error en tarea {task['product_name']}: {e}")
                finally:
                    # Marcar tarea como completada pase lo que pase
                    task_queue.task_done()
            
            # Limpieza: El worker cierra SUS propios drivers al terminar
            logger.debug(f"[WORKER {worker_id}] Terminando y limpiando drivers...")
            for sm_name, ext_instance in local_extractors.items():
                try:
                    if hasattr(ext_instance, 'driver') and ext_instance.driver:
                        ext_instance.driver.quit()
                except Exception as e:
                    logger.error(f"Error cerrando driver {sm_name} en worker {worker_id}: {e}")

        # 4. Lanzar los Hilos (Workers)
        threads = []
        # No crear más workers que tareas
        num_workers = min(self.max_workers, total_tasks)
        if num_workers < 1: num_workers = 1

        for i in range(num_workers):
            t = threading.Thread(target=worker_loop, args=(i,))
            t.start()
            threads.append(t)
            
        # 5. Esperar a que todos terminen
        for t in threads:
            t.join()
            
        logger.info("[EXTRACT] Todos los workers han finalizado.")
        
        # 6. Consolidar y Agrupar resultados
        # 6. Consolidar y Agrupar resultados
        final_results = {}
        if results_list:
            # Unimos toda la lista en un gran DataFrame
            full_df = pd.concat(results_list, ignore_index=True)
            
            # Separamos por supermercado para devolver el formato esperado por main.py (Dict[str, DataFrame])
            for sm in valid_supers:
                # --- CORRECCIÓN AQUÍ: Usar 'supermercado' en lugar de 'supermarket' ---
                if 'supermercado' in full_df.columns:
                    sm_df = full_df[full_df['supermercado'] == sm]
                    if not sm_df.empty:
                        final_results[sm] = sm_df
                else:
                    logger.critical("La columna 'supermercado' no existe en el DataFrame consolidado. Columnas encontradas: %s", full_df.columns.tolist())
        
        elapsed = time.time() - start_time
        logger.info(f"[EXTRACT] Proceso total completado en {elapsed:.2f}s")
        return final_results

    def _process_single_task(self, extractor, task: Dict) -> pd.DataFrame:
        """
        Versión Corregida: Sin normalización a diccionario.
        Inyecta metadatos directamente en el DataFrame.
        """
        url = task['url']
        supermarket = task['supermarket']
        
        # 1. Verificar Caché
        cache_key = f"{supermarket}_{url}"
        cached_result = self.result_cache.get(cache_key, max_age_hours=24)
        
        raw_data = None
        if cached_result:
            raw_data = cached_result
        else:
            try:
                # Extracción real
                raw_data = extractor.extraer_producto(url)
                
                # Cachear si no es un error y tiene datos
                if raw_data is not None:
                    is_error = False
                    if isinstance(raw_data, dict) and 'error_type' in raw_data:
                        is_error = True
                    # Si es DataFrame vacío
                    if isinstance(raw_data, pd.DataFrame) and raw_data.empty:
                        is_error = True
                        
                    if not is_error:
                        self.result_cache.set(cache_key, raw_data)
            except Exception as e:
                raw_data = pd.DataFrame([{'error_type': f'EXCEPTION: {str(e)}'}])

        # 2. CONVERTIR A DATAFRAME (Sin to_dict intermedio)
        df_result = pd.DataFrame()
        
        if raw_data is None:
            df_result = pd.DataFrame([{'error_type': 'SIN_DATOS'}])
        elif isinstance(raw_data, pd.DataFrame):
            df_result = raw_data.copy()
            if df_result.empty: # Si el extractor devolvió DF vacío
                df_result = pd.DataFrame([{'error_type': 'SIN_DATOS'}])
        elif isinstance(raw_data, dict):
            df_result = pd.DataFrame([raw_data])
        elif isinstance(raw_data, pd.Series):
            df_result = raw_data.to_frame().T
        else:
            # Fallback para tipos desconocidos
            df_result = pd.DataFrame([{'error_type': 'FORMATO_DESCONOCIDO'}])

        # 3. INYECCIÓN FORZADA DE METADATOS (Aquí solucionamos el bug)
        # Asignamos las columnas directamente. Si existen, se sobrescriben. Si no, se crean.
        # Usamos values constantes para todas las filas del DF (generalmente es 1 fila)
        df_result['supermercado'] = supermarket
        df_result['producto_nombre'] = task['product_name']
        df_result['url'] = url
        df_result['peso'] = task['peso']
        df_result['unidad_medida'] = task['unidad']
        df_result['fecha_extraccion'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 4. Asegurar campos mínimos (precio, nombre, etc) para que no falle Transform
        # Rellenamos NaNs con valores por defecto
        required_cols = {
            'nombre': '', 
            'precio_normal': '', 
            'precio_descuento': '', 
            'descuentos': 'Ninguno',
            'fecha': datetime.today().strftime("%Y-%m-%d")
        }
        
        for col, default_val in required_cols.items():
            if col not in df_result.columns:
                df_result[col] = default_val
            else:
                # Rellenar nulos en esas columnas específicas
                df_result[col] = df_result[col].fillna(default_val)

        return df_result
    # --- Métodos Auxiliares Originales ---
    
    def _ensure_required_fields(self, data: Dict) -> Dict:
        """Asegura campos por defecto"""
        defaults = {
            'nombre': '', 'precio_normal': '', 'precio_descuento': '', 
            'precio_por_unidad': '', 'descuentos': 'Ninguno', 
            'fecha': datetime.today().strftime("%Y-%m-%d"),
            'supermercado': '', 'url': '', 'error_type': ''
        }
        for k, v in defaults.items():
            if k not in data or data[k] is None: 
                data[k] = v
        return data

    def _parse_sheet_data(self, df_sheet: pd.DataFrame, sheet_name: str) -> Dict[str, List[Dict]]:
        """Logica de parseo del Excel original"""
        products = {}
        for idx, row in df_sheet.iterrows():
            if len(row) >= 2 and pd.notna(row[0]) and str(row[0]).strip():
                p_name = str(row[0]).strip()
                link = row[1] if len(row) > 1 else None
                if pd.notna(link) and isinstance(link, str) and self._is_valid_url(link.strip()):
                    info = {
                        'url': link.strip(),
                        'peso': self._clean_peso_value(row[2]) if len(row)>2 else '',
                        'unidad': self._clean_unidad_value(row[3]) if len(row)>3 else ''
                    }
                    if p_name in products: 
                        products[p_name].append(info)
                    else: 
                        products[p_name] = [info]
        return products

    def _clean_peso_value(self, val):
        if pd.isna(val): return ""
        s = str(val).strip()
        # Requiere import re
        s = re.sub(r'[^\d,.]', '', s).replace(',', '.')
        try:
            float(s)
            return s
        except:
            return ""

    def _clean_unidad_value(self, val):
        if pd.isna(val): return ""
        s = str(val).strip().upper()
        m = {'GRAMOS': 'G', 'GR': 'G', 'LITROS': 'L', 'UNIDADES': 'UN', 'U': 'UN'}
        return m.get(s, s)

    def _is_valid_url(self, text: str) -> bool:
        return text.startswith(('http://', 'https://'))