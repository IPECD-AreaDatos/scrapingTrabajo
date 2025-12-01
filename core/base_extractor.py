"""
Clase base abstracta para extractores ETL
Define la interfaz común para todos los extractores
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging


class BaseExtractor(ABC):
    """
    Clase base para todos los extractores del sistema ETL.
    
    Cada módulo debe heredar de esta clase e implementar el método extract().
    """
    
    def __init__(self, module_name: str):
        """
        Inicializa el extractor base.
        
        Args:
            module_name: Nombre del módulo ETL (para logging)
        """
        self.module_name = module_name
        self.logger = logging.getLogger(f"{__name__}.{module_name}")
        self.logger.info(f"[EXTRACT] Inicializando extractor para {module_name}")
    
    @abstractmethod
    def extract(self, **kwargs) -> Any:
        """
        Método abstracto que debe implementar cada extractor.
        
        Returns:
            Datos extraídos en el formato que requiera el transformer.
            Puede ser DataFrame, dict, list, etc.
        
        Raises:
            Exception: Si la extracción falla
        """
        pass
    
    def validate_extraction(self, data: Any) -> bool:
        """
        Valida que los datos extraídos sean válidos.
        
        Args:
            data: Datos a validar
            
        Returns:
            True si los datos son válidos, False en caso contrario
        """
        if data is None:
            self.logger.error("[EXTRACT] Los datos extraídos son None")
            return False
        
        # Validación básica: verificar que no esté vacío
        if isinstance(data, (list, dict, str)):
            if len(data) == 0:
                self.logger.error("[EXTRACT] Los datos extraídos están vacíos")
                return False
        
        self.logger.info("[EXTRACT] Validación de datos exitosa")
        return True
    
    def cleanup(self) -> None:
        """
        Limpia recursos utilizados por el extractor.
        Sobrescribir en subclases si es necesario (ej: cerrar navegadores, conexiones).
        """
        self.logger.debug(f"[EXTRACT] Limpiando recursos de {self.module_name}")



