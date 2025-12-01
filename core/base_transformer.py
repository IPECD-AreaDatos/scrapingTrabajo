"""
Clase base abstracta para transformers ETL
Define la interfaz común para todas las transformaciones
"""
from abc import ABC, abstractmethod
from typing import Any, Optional
import logging
import pandas as pd


class BaseTransformer(ABC):
    """
    Clase base para todos los transformers del sistema ETL.
    
    Cada módulo debe heredar de esta clase e implementar el método transform().
    """
    
    def __init__(self, module_name: str):
        """
        Inicializa el transformer base.
        
        Args:
            module_name: Nombre del módulo ETL (para logging)
        """
        self.module_name = module_name
        self.logger = logging.getLogger(f"{__name__}.{module_name}")
        self.logger.info(f"[TRANSFORM] Inicializando transformer para {module_name}")
    
    @abstractmethod
    def transform(self, data: Any, **kwargs) -> pd.DataFrame:
        """
        Método abstracto que debe implementar cada transformer.
        
        Args:
            data: Datos extraídos (puede ser DataFrame, dict, list, etc.)
            **kwargs: Argumentos adicionales específicos del módulo
            
        Returns:
            DataFrame transformado y listo para cargar
            
        Raises:
            Exception: Si la transformación falla
        """
        pass
    
    def validate_transformation(self, df: pd.DataFrame) -> bool:
        """
        Valida que el DataFrame transformado sea válido.
        
        Args:
            df: DataFrame a validar
            
        Returns:
            True si el DataFrame es válido, False en caso contrario
        """
        if df is None:
            self.logger.error("[TRANSFORM] El DataFrame transformado es None")
            return False
        
        if not isinstance(df, pd.DataFrame):
            self.logger.error("[TRANSFORM] El resultado no es un DataFrame")
            return False
        
        if df.empty:
            self.logger.warning("[TRANSFORM] El DataFrame transformado está vacío")
            return False
        
        self.logger.info(
            f"[TRANSFORM] Validación exitosa: {len(df)} filas, {len(df.columns)} columnas"
        )
        return True
    
    def get_transformation_stats(self, df: pd.DataFrame) -> dict:
        """
        Obtiene estadísticas del DataFrame transformado.
        
        Args:
            df: DataFrame a analizar
            
        Returns:
            Diccionario con estadísticas
        """
        return {
            'rows': len(df),
            'columns': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024**2,
            'null_values': df.isnull().sum().to_dict(),
        }



