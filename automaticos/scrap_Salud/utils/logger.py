"""
Logger centralizado para el proyecto de Salud - Seguimiento de Embarazo.
Uso: from utils.logger import setup_logger
"""
import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(nombre_log: str, level: int = logging.INFO) -> None:
    """
    Configura el sistema de logging con rotación de archivos.

    Args:
        nombre_log: Nombre base del archivo de log (ej: 'salud_embarazo')
        level: Nivel de logging (default: INFO)
    """
    # Directorio de logs: sube dos niveles desde utils/ para ir a la raíz o carpeta logs/
    # Ajustado para que sea consistente con tu estructura de carpetas
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    log_dir = os.path.join(base_dir, 'logs')
    
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, f'{nombre_log}.log')

    # Handler para archivo: 5MB por archivo, guarda hasta 2 backups
    handler_file = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=2,
        encoding='utf-8'
    )
    handler_file.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))

    # Handler para consola: Más limpio para leer mientras debugueas
    handler_console = logging.StreamHandler()
    handler_console.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    ))

    # Configuración global
    logging.basicConfig(
        level=level, 
        handlers=[handler_file, handler_console],
        force=True  # Asegura que se aplique la configuración incluso si ya existe un logger
    )
    
    logger = logging.getLogger(__name__)
    logger.info("Sistema de logging inicializado en: %s", log_file)