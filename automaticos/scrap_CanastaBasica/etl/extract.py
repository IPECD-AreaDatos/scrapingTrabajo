"""
Módulo de Extracción para Canasta Básica
Responsabilidad: Leer links de BASE DE DATOS e extraer productos
"""
import os
import sys
import pandas as pd
import logging
import time
import threading
import queue
from datetime import datetime
from typing import Dict, List
from dotenv import load_dotenv

# Agregar directorio padre
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.utils_db import ConexionBaseDatos  # Usamos DB en lugar de Sheets
from utils.cookie_manager import CookieManager
from utils.optimization import ResultCache, set_parallel_mode
from extractors.carrefour_extractor import CarrefourExtractor
from extractors.delimart_extractor import DelimartExtractor
from extractors.masonline_extractor import MasonlineExtractor
from extractors.depot_extractor import DepotExtractor
from extractors.lareina_extractor import LareinaExtractor
from extractors.dia_extractor import DiaExtractor
from extractors.paradacanga_extractor import ParadacangaExtractor

logger = logging.getLogger(__name__)

class ExtractCanastaBasica:
    def __init__(self, enable_parallel: bool = True, max_workers: int = 10):
        load_dotenv()
        self.enable_parallel = enable_parallel
        self.max_workers = max_workers if enable_parallel else 1
        
        # Mapeo de clases
        self.extractor_classes = {
            'Carrefour': CarrefourExtractor,
            'Delimart': DelimartExtractor,
            'MasOnline': MasonlineExtractor,
            'Depot': DepotExtractor, 
            'La Reina': LareinaExtractor,
            'DIA%': DiaExtractor, 
            'Parada Canga': ParadacangaExtractor
        }

        # Inicializar DB para leer links
        self.db = ConexionBaseDatos(
            host=os.getenv('HOST_DBB'),
            user=os.getenv('USER_DBB'),
            password=os.getenv('PASSWORD_DBB'),
            database='canasta_basica_supermercados'
        )

        # Cookies y Cache
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.cookie_manager = CookieManager(base_dir)
        cache_dir = os.path.join(base_dir, 'cache')
        self.result_cache = ResultCache(cache_dir)

    def read_links_from_db(self, supermercado_filtro=None) -> List[Dict]:
        """
        Lee los links activos directamente de la base de datos.
        Retorna una lista de diccionarios con la info necesaria.
        """
        logger.info("[EXTRACT] Leyendo links de la base de datos...")
        
        if not self.db.connect_db():
            raise Exception("No se pudo conectar a la BD para leer links")

        # Query optimizada con JOIN para traer el nombre del super
        query = """
            SELECT 
                lp.id_link_producto, 
                lp.link, 
                lp.peso, 
                lp.unidad_medida, 
                s.nombre as nombre_super
            FROM link_productos lp
            JOIN supermercados s ON lp.id_supermercado = s.id_super
            WHERE lp.activo = 1
        """
        
        params = None
        if supermercado_filtro:
            query += " AND s.nombre = %s"
            params = (supermercado_filtro,)
        
        try:
            # Ejecutar query usando pandas para facilitar el manejo
            df_links = pd.read_sql(query, self.db.connection, params=params)
            
            # Convertir a lista de diccionarios para el procesamiento
            links_list = df_links.to_dict('records')
            logger.info(f"[EXTRACT] Se obtuvieron {len(links_list)} links activos.")
            return links_list
            
        except Exception as e:
            logger.error(f"Error leyendo links de DB: {e}")
            return []
        finally:
            self.db.close_connections()

    def extract(self, links_list: List[Dict]) -> pd.DataFrame:
        """Proceso principal de extracción"""
        logger.info(f"[EXTRACT] Iniciando extracción con {self.max_workers} workers...")
        
        # Activar modo paralelo para evitar limpiezas globales accidentales que maten otros browsers
        set_parallel_mode(True)
        
        start_time = time.time()
        
        try:
            # 1. Llenar la cola
            task_queue = queue.Queue()
            
            for item in links_list:
                # Mapear nombre de DB a clave del extractor (normalizando si es necesario)
                # El diccionario extractor_classes usa los nombres exactos de la BD ahora
                super_name = item['nombre_super']
                
                if super_name in self.extractor_classes:
                    task = {
                        'id_link_producto': item['id_link_producto'], # VITAL para la carga
                        'supermarket': super_name,
                        'url': item['link'],
                        'peso_db': item['peso'], # Peso definido en la DB
                        'unidad_db': item['unidad_medida'] # Unidad definida en la DB
                    }
                    task_queue.put(task)
                else:
                    logger.warning(f"No hay extractor configurado para: {super_name}")

            total_tasks = task_queue.qsize()
            logger.info(f"[EXTRACT] Cola generada con {total_tasks} tareas.")

            results_list = []
            results_lock = threading.Lock()

            # 2. Worker Loop
            def worker_loop(worker_id):
                local_extractors = {}
                while True:
                    try:
                        task = task_queue.get(block=False)
                    except queue.Empty:
                        break
                    
                    sm = task['supermarket']
                    
                    try:
                        if sm not in local_extractors:
                            # Instanciamos la clase (ej: LareinaExtractor)
                            local_extractors[sm] = self.extractor_classes[sm]()

                        df_result = self._process_single_task(local_extractors[sm], task)
                        
                        if not df_result.empty:
                            with results_lock:
                                results_list.append(df_result)
                    
                    except Exception as e:
                        logger.error(f"[WORKER {worker_id}] Error: {e}")
                    finally:
                        task_queue.task_done()
                
                # Limpieza del worker
                for ext in local_extractors.values():
                    if hasattr(ext, 'cleanup_driver'): ext.cleanup_driver()
                    elif hasattr(ext, 'driver') and ext.driver: ext.driver.quit()

            # 3. Lanzar hilos
            threads = []
            num_workers = min(self.max_workers, total_tasks)
            if num_workers < 1: num_workers = 1

            for i in range(num_workers):
                t = threading.Thread(target=worker_loop, args=(i,))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            # 4. Consolidar
            final_df = pd.DataFrame()
            if results_list:
                final_df = pd.concat(results_list, ignore_index=True)
            
            elapsed = time.time() - start_time
            logger.info(f"[EXTRACT] Finalizado en {elapsed:.2f}s. Registros extraídos: {len(final_df)}")
            return final_df
        finally:
            # Desactivar modo paralelo al terminar
            set_parallel_mode(False)

    def _process_single_task(self, extractor, task) -> pd.DataFrame:
        url = task['url']
        
        # Intentar extracción
        try:
            raw_data = extractor.extraer_producto(url)
        except Exception as e:
            raw_data = {'error_type': str(e)}

        # Convertir a DataFrame
        if isinstance(raw_data, dict):
            df = pd.DataFrame([raw_data])
        else:
            df = pd.DataFrame([{'error_type': 'SIN_DATOS'}])

        if df.empty: return df

        # INYECTAR DATOS DE LA BASE (CLAVE FORÁNEA)
        df['id_link_producto'] = task['id_link_producto']
        
        # Prioridad de datos: Si el scraper no trae peso, usar el de la DB
        if 'peso' not in df.columns or not df['peso'].iloc[0]:
            df['peso'] = task['peso_db']
        if 'unidad' not in df.columns or not df['unidad'].iloc[0]:
            df['unidad'] = task['unidad_db']

        return df