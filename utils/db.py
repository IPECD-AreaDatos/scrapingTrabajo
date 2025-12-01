"""
Utilidades para conexión a bases de datos
"""
from typing import Optional, Dict, Any
import pymysql
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine
import pandas as pd
import logging
from config.settings import Settings


class DatabaseConnection:
    """
    Clase para manejar conexiones a bases de datos MySQL.
    Proporciona métodos para conectar, ejecutar queries y cargar DataFrames.
    """
    
    def __init__(
        self,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        database: Optional[str] = None,
        database_type: str = 'economico'
    ):
        """
        Inicializa la conexión a la base de datos.
        
        Args:
            host: Host de la base de datos (opcional, usa settings si no se proporciona)
            user: Usuario de la base de datos (opcional)
            password: Contraseña (opcional)
            database: Nombre de la base de datos (opcional)
            database_type: Tipo de base de datos ('economico', 'socio', 'dwh_socio')
        """
        settings = Settings()
        
        if database_type and not all([host, user, password, database]):
            db_config = settings.get_db_config(database_type)
            self.host = db_config['host']
            self.user = db_config['user']
            self.password = db_config['password']
            self.database = db_config['database']
        else:
            self.host = host or settings.DB_HOST
            self.user = user or settings.DB_USER
            self.password = password or settings.DB_PASSWORD
            self.database = database or settings.DB_DATALAKE_ECONOMICO
        
        self.connection: Optional[pymysql.Connection] = None
        self.cursor: Optional[pymysql.cursors.Cursor] = None
        self.engine: Optional[Engine] = None
        self.logger = logging.getLogger(f"{__name__}.{self.database}")
    
    def connect(self) -> None:
        """Establece conexión a la base de datos"""
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4'
            )
            self.cursor = self.connection.cursor()
            self.logger.info(f"Conexión establecida a {self.database}")
        except Exception as e:
            self.logger.error(f"Error al conectar a la base de datos: {e}")
            raise
    
    def get_engine(self) -> Engine:
        """Obtiene un engine de SQLAlchemy para operaciones con pandas"""
        if not self.engine:
            connection_string = (
                f"mysql+pymysql://{self.user}:{self.password}@"
                f"{self.host}:3306/{self.database}"
            )
            self.engine = create_engine(connection_string, echo=False)
        return self.engine
    
    def execute_query(self, query: str, fetch: bool = True) -> Optional[list]:
        """
        Ejecuta una query SQL.
        
        Args:
            query: Query SQL a ejecutar
            fetch: Si True, retorna los resultados
            
        Returns:
            Resultados de la query o None
        """
        if not self.connection:
            self.connect()
        
        try:
            self.cursor.execute(query)
            if fetch:
                return self.cursor.fetchall()
            return None
        except Exception as e:
            self.logger.error(f"Error al ejecutar query: {e}")
            raise
    
    def table_exists(self, table_name: str) -> bool:
        """
        Verifica si una tabla existe en la base de datos.
        
        Args:
            table_name: Nombre de la tabla
            
        Returns:
            True si la tabla existe, False en caso contrario
        """
        try:
            engine = self.get_engine()
            inspector = inspect(engine)
            return table_name in inspector.get_table_names()
        except Exception as e:
            self.logger.error(f"Error al verificar existencia de tabla: {e}")
            return False
    
    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: str = 'append',
        index: bool = False
    ) -> bool:
        """
        Carga un DataFrame en una tabla de la base de datos.
        
        Args:
            df: DataFrame a cargar
            table_name: Nombre de la tabla destino
            if_exists: Comportamiento si la tabla existe ('fail', 'replace', 'append')
            index: Si True, incluye el índice del DataFrame
            
        Returns:
            True si la carga fue exitosa
        """
        try:
            engine = self.get_engine()
            df.to_sql(
                table_name,
                con=engine,
                if_exists=if_exists,
                index=index,
                method='multi',
                chunksize=1000
            )
            self.logger.info(f"Datos cargados en {table_name}: {len(df)} filas")
            return True
        except Exception as e:
            self.logger.error(f"Error al cargar DataFrame: {e}")
            raise
    
    def get_max_date(self, table_name: str, date_column: str = 'fecha') -> Optional[pd.Timestamp]:
        """
        Obtiene la fecha máxima de una tabla.
        
        Args:
            table_name: Nombre de la tabla
            date_column: Nombre de la columna de fecha
            
        Returns:
            Fecha máxima o None si no hay datos
        """
        try:
            query = f"SELECT MAX({date_column}) FROM {table_name}"
            result = self.execute_query(query)
            if result and result[0] and result[0][0]:
                return pd.to_datetime(result[0][0])
            return None
        except Exception as e:
            self.logger.warning(f"Error al obtener fecha máxima: {e}")
            return None
    
    def commit(self) -> None:
        """Confirma la transacción"""
        if self.connection:
            self.connection.commit()
            self.logger.debug("Transacción confirmada")
    
    def rollback(self) -> None:
        """Revierte la transacción"""
        if self.connection:
            self.connection.rollback()
            self.logger.warning("Transacción revertida")
    
    def close(self) -> None:
        """Cierra la conexión a la base de datos"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.connection:
                self.connection.close()
            if self.engine:
                self.engine.dispose()
            self.logger.info("Conexión cerrada")
        except Exception as e:
            self.logger.warning(f"Error al cerrar conexión: {e}")
    
    def __enter__(self):
        """Context manager: entrada"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager: salida"""
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()



