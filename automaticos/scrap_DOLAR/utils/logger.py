"""
Logger centralizado para scrapers del proyecto IPECD.
Uso: from utils.logger import setup_logger
"""
import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(nombre_log: str, level: int = logging.INFO) -> None:
    """
    Configura el sistema de logging con rotación de archivos.

    Args:
        nombre_log: Nombre base del archivo de log (ej: 'dolar_scraper')
        level: Nivel de logging (default: INFO)
    """
    log_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '..', '..', 'logs'
    )
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, f'{nombre_log}.log')

    handler_file = RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=2,
        encoding='utf-8'
    )
    handler_file.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))

    handler_console = logging.StreamHandler()
    handler_console.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    ))

    logging.basicConfig(level=level, handlers=[handler_file, handler_console])
