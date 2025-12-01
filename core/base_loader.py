"""
Clase base abstracta para loaders ETL
Define la interfaz común para todas las cargas de datos
"""
from abc import ABC, abstractmethod
from typing import Any, Optional, Dict
import logging
import pandas as pd


class BaseLoader(ABC):
    """
    Clase base para todos los loaders del sistema ETL.
    
    Cada módulo debe heredar de esta clase e implementar el método load().
    """
    
    def __init__(self, module_name: str, db_config: Optional[Dict[str, str]] = None):
        """
        Inicializa el loader base.
        
        Args:
            module_name: Nombre del módulo ETL (para logging)
            db_config: Configuración de base de datos (host, user, password, database)
        """
        self.module_name = module_name
        self.db_config = db_config
        self.logger = logging.getLogger(f"{__name__}.{module_name}")
        self.logger.info(f"[LOAD] Inicializando loader para {module_name}")
        self._connection = None
    
    @abstractmethod
    def load(self, data: pd.DataFrame, **kwargs) -> bool:
        """
        Método abstracto que debe implementar cada loader.
        
        Args:
            data: DataFrame transformado a cargar
            **kwargs: Argumentos adicionales específicos del módulo
            
        Returns:
            True si la carga fue exitosa, False en caso contrario
            
        Raises:
            Exception: Si la carga falla
        """
        pass
    
    def validate_load_data(self, df: pd.DataFrame) -> bool:
        """
        Valida que los datos a cargar sean válidos.
        
        Args:
            df: DataFrame a validar
            
        Returns:
            True si los datos son válidos, False en caso contrario
        """
        if df is None:
            self.logger.error("[LOAD] El DataFrame a cargar es None")
            return False
        
        if not isinstance(df, pd.DataFrame):
            self.logger.error("[LOAD] Los datos a cargar no son un DataFrame")
            return False
        
        if df.empty:
            self.logger.warning("[LOAD] El DataFrame a cargar está vacío")
            return False
        
        self.logger.info(f"[LOAD] Validación exitosa: {len(df)} filas para cargar")
        return True
    
    def close_connections(self) -> None:
        """
        Cierra conexiones abiertas (BD, APIs, etc.).
        Sobrescribir en subclases si es necesario.
        """
        if self._connection:
            try:
                self._connection.close()
                self.logger.debug(f"[LOAD] Conexión cerrada para {self.module_name}")
            except Exception as e:
                self.logger.warning(f"[LOAD] Error al cerrar conexión: {e}")
        self.logger.debug(f"[LOAD] Limpiando recursos de {self.module_name}")



