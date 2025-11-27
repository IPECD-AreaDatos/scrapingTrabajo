"""
Módulo de Carga para Canasta Básica
Responsabilidad: Guardar datos en CSV y cargar a base de datos
"""
import os
import sys
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv

# Agregar directorio padre al path para imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.utils_db import ConexionBaseDatos

logger = logging.getLogger(__name__)

# Directorios base para salidas
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILES_DIR = os.path.join(BASE_DIR, 'files')
os.makedirs(FILES_DIR, exist_ok=True)


class LoadCanastaBasica:
    """Clase para manejar la carga de datos de Canasta Básica"""
    
    def __init__(self):
        load_dotenv()
        self.db = None
        self.base_dir = BASE_DIR
        self.files_dir = FILES_DIR
        self._setup_database()
    
    def _setup_database(self):
        """Configura la conexión a base de datos"""
        try:
            host = os.getenv('HOST_DBB')
            user = os.getenv('USER_DBB')
            password = os.getenv('PASSWORD_DBB')
            database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')
            
            if all([host, user, password, database]):
                self.db = ConexionBaseDatos(host, user, password, database)
                self.db.connect_db()
                logger.info("[OK] Conexión a base de datos establecida")
            else:
                logger.warning("[WARNING] Variables de entorno de BD no configuradas")
                self.db = None
        except Exception as e:
            logger.error("[ERROR] Error configurando base de datos: %s", str(e))
            self.db = None
    
    def save_to_csv(self, df: pd.DataFrame, filename: str = None) -> str:
        """Guarda el DataFrame en un archivo CSV"""
        if df.empty:
            logger.warning("[WARNING] No hay datos para guardar en CSV")
            return None
        
        if filename is None:
            filename = f'canasta_basica_completa_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
        
        output_path = os.path.join(self.files_dir, filename)
        
        try:
            df.to_csv(output_path, index=False, encoding='utf-8')
            logger.info("[OK] Datos guardados en CSV: %s (%d registros)", output_path, len(df))
            return output_path
        except Exception as e:
            logger.error("[ERROR] Error guardando CSV: %s", str(e))
            return None
    
    def save_individual_csvs(self, all_supermarkets_data: dict, df: pd.DataFrame):
        """Guarda CSVs individuales por supermercado"""
        for supermarket, supermarket_df in all_supermarkets_data.items():
            if not supermarket_df.empty:
                csv_file = f'productos_{supermarket}_{datetime.now().strftime("%Y%m%d_%H%M")}.csv'
                output_path = os.path.join(self.files_dir, csv_file)
                try:
                    supermarket_df.to_csv(output_path, index=False, encoding='utf-8')
                    logger.info("[OK] Datos de %s guardados en %s", supermarket, output_path)
                except Exception as e:
                    logger.error("[ERROR] Error guardando CSV de %s: %s", supermarket, str(e))
    
    def load_to_database(self, df: pd.DataFrame) -> bool:
        """Carga datos a la base de datos"""
        if df.empty:
            logger.warning("[WARNING] No hay datos para cargar a base de datos")
            return False
        
        if not self.db:
            logger.warning("[WARNING] No hay conexión a base de datos")
            return False
        
        try:
            # Cargar datos directamente usando la conexión
            success = self.db.insert_append(
                table_name="canasta_basica",
                df=df
            )
            
            if success:
                logger.info("[OK] Carga a base de datos completada: %d registros", len(df))
            else:
                logger.warning("[WARNING] No se cargaron datos nuevos a la base de datos")
            return success
        except Exception as e:
            logger.error("[ERROR] Error cargando a base de datos: %s", str(e))
            return False
    
    def load(self, df: pd.DataFrame, all_supermarkets_data: dict = None, skip_database: bool = False) -> bool:
        """Ejecuta el proceso completo de carga"""
        logger.info("[LOAD] Iniciando proceso de carga...")
        
        # Guardar CSV consolidado
        csv_file = self.save_to_csv(df)
        
        # Guardar CSVs individuales si se proporcionan
        if all_supermarkets_data:
            self.save_individual_csvs(all_supermarkets_data, df)
        
        # Cargar a base de datos (solo si no se omite)
        db_success = True
        if not skip_database:
            db_success = self.load_to_database(df)
        else:
            logger.info("[LOAD] Carga a base de datos omitida (skip_database=True)")
        
        # Mostrar resumen
        self._show_summary(df)
        
        return db_success
    
    def _show_summary(self, df: pd.DataFrame):
        """Muestra resumen de los datos cargados"""
        if df.empty:
            return
        
        logger.info("[LOAD] Resumen de datos:")
        logger.info("[LOAD]   Total de registros: %d", len(df))
        
        # Resumen por supermercado
        if 'supermercado' in df.columns:
            supermarket_counts = df['supermercado'].value_counts()
            logger.info("[LOAD]   Registros por supermercado:")
            for supermarket, count in supermarket_counts.items():
                logger.info("[LOAD]     %s: %d", supermarket, count)
        
        # Resumen de productos con precios
        if 'precio_normal' in df.columns:
            products_with_prices = df[df['precio_normal'].notna() & (df['precio_normal'] != '')]
            logger.info("[LOAD]   Productos con precios extraídos: %d/%d", 
                    len(products_with_prices), len(df))
            
            # Productos sin precios
            products_without_prices = df[df['precio_normal'].isna() | (df['precio_normal'] == '')]
            if not products_without_prices.empty:
                logger.warning("[LOAD]   Productos sin precios: %d", len(products_without_prices))
    
    def close_connections(self):
        """Cierra las conexiones a la base de datos"""
        if self.db:
            try:
                self.db.close_connections()
                logger.info("[OK] Conexión a BD cerrada")
            except Exception as e:
                logger.error("[ERROR] Error cerrando conexión: %s", str(e))

