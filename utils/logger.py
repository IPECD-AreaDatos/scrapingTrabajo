"""
Sistema de logging centralizado con rotación de archivos
"""
import logging
import logging.handlers
from pathlib import Path
from typing import Optional
from config.settings import Settings


def setup_logger(
    module_name: str,
    log_level: Optional[str] = None,
    log_to_file: bool = True,
    log_to_console: bool = True
) -> logging.Logger:
    """
    Configura un logger para un módulo específico con rotación de archivos.
    
    Args:
        module_name: Nombre del módulo (ej: 'ventas_combustible')
        log_level: Nivel de logging (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Si True, escribe logs a archivo
        log_to_console: Si True, escribe logs a consola
        
    Returns:
        Logger configurado
    """
    settings = Settings()
    
    # Nivel de logging
    level = log_level or settings.LOG_LEVEL
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Crear logger
    logger = logging.getLogger(module_name)
    logger.setLevel(numeric_level)
    
    # Evitar duplicar handlers
    if logger.handlers:
        return logger
    
    # Formato de logs
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler para archivo con rotación
    if log_to_file:
        log_file = settings.LOGS_DIR / f"{module_name}.log"
        
        # RotatingFileHandler: rota a medianoche y mantiene 30 días
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=str(log_file),
            when=settings.LOG_ROTATION,
            interval=1,
            backupCount=settings.LOG_RETENTION_DAYS,
            encoding='utf-8'
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    # Handler para consola
    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    
    return logger


def get_logger(module_name: str) -> logging.Logger:
    """
    Obtiene un logger existente o crea uno nuevo.
    
    Args:
        module_name: Nombre del módulo
        
    Returns:
        Logger
    """
    logger = logging.getLogger(module_name)
    
    # Si no tiene handlers, configurarlo
    if not logger.handlers:
        return setup_logger(module_name)
    
    return logger



