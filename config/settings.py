"""
Configuración global del proyecto ETL
Maneja variables de entorno y configuraciones centralizadas
"""
import os
from typing import Optional
from pathlib import Path
from dotenv import load_dotenv


class Settings:
    """Clase singleton para manejar configuración global"""
    
    _instance: Optional['Settings'] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        # Cargar variables de entorno
        load_dotenv()
        
        # Directorios base
        self.BASE_DIR = Path(__file__).parent.parent
        self.LOGS_DIR = self.BASE_DIR / 'logs'
        self.OUTPUT_DIR = self.BASE_DIR / 'output'
        self.MODULES_DIR = self.BASE_DIR / 'modules'
        
        # Crear directorios si no existen
        self.LOGS_DIR.mkdir(exist_ok=True)
        self.OUTPUT_DIR.mkdir(exist_ok=True)
        
        # Base de datos - Datalake Económico
        self.DB_HOST = os.getenv('HOST_DBB')
        self.DB_USER = os.getenv('USER_DBB')
        self.DB_PASSWORD = os.getenv('PASSWORD_DBB')
        self.DB_DATALAKE_ECONOMICO = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')
        self.DB_DATALAKE_SOCIO = os.getenv('NAME_DBB_DATALAKE_SOCIO')
        self.DB_DWH_SOCIO = os.getenv('NAME_DBB_DWH_SOCIO')
        
        # Google Sheets
        self.GOOGLE_SHEETS_API_KEY = os.getenv('GOOGLE_SHEETS_API_KEY')
        
        # Logging
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        self.LOG_ROTATION = os.getenv('LOG_ROTATION', 'midnight')
        self.LOG_RETENTION_DAYS = int(os.getenv('LOG_RETENTION_DAYS', '30'))
        
        # Ejecución
        self.EXECUTION_HISTORY_FILE = self.BASE_DIR / 'logs' / 'execution_history.json'
        
        # Validar variables críticas
        self._validate_required_vars()
        
        self._initialized = True
    
    def _validate_required_vars(self) -> None:
        """Valida que las variables de entorno críticas estén definidas"""
        required_vars = {
            'DB_HOST': self.DB_HOST,
            'DB_USER': self.DB_USER,
            'DB_PASSWORD': self.DB_PASSWORD,
        }
        
        missing = [var for var, value in required_vars.items() if not value]
        if missing:
            raise ValueError(
                f"Variables de entorno faltantes: {', '.join(missing)}. "
                "Verifica tu archivo .env"
            )
    
    def get_db_config(self, database_type: str = 'economico') -> dict:
        """Obtiene configuración de base de datos por tipo"""
        db_name_map = {
            'economico': self.DB_DATALAKE_ECONOMICO,
            'socio': self.DB_DATALAKE_SOCIO,
            'dwh_socio': self.DB_DWH_SOCIO,
        }
        
        db_name = db_name_map.get(database_type)
        if not db_name:
            raise ValueError(f"Tipo de base de datos inválido: {database_type}")
        
        return {
            'host': self.DB_HOST,
            'user': self.DB_USER,
            'password': self.DB_PASSWORD,
            'database': db_name,
        }



