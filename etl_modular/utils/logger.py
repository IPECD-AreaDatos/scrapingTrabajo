import logging
import os

def setup_logger(name="etl"):
    """
    Configura y retorna un objeto logger.

    El 'name' se usa tanto para el nombre del archivo de log
    como para el nombre del logger para permitir logs modulares.
    """
    logs_dir = "logs"
    os.makedirs(logs_dir, exist_ok=True)
    log_file_path = os.path.join(logs_dir, f"{name}.log")

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO) # Establece el nivel del logger

    # Evita añadir múltiples handlers si ya existen
    if not logger.handlers:
        # Crea un FileHandler para el archivo de log
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setLevel(logging.INFO) # Nivel del handler

        # Define el formato
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        # Añade el handler al logger
        logger.addHandler(file_handler)
        
        # Opcional: Si también quieres ver los logs en la consola
        # stream_handler = logging.StreamHandler()
        # stream_handler.setLevel(logging.INFO)
        # stream_handler.setFormatter(formatter)
        # logger.addHandler(stream_handler)

    return logger