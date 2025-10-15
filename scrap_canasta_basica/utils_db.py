"""
Implementación local de ConexionBaseDatos para reemplazar etl_modular
"""
import pymysql
from sqlalchemy import create_engine, text
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class ConexionBaseDatos:
    """Clase para manejar conexiones a base de datos MySQL"""
    
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.engine = None
        
    def connect_db(self):
        """Establece conexión con la base de datos"""
        try:
            # Conexión directa con pymysql
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
                charset='utf8mb4'
            )
            
            # Engine de SQLAlchemy para operaciones con pandas
            connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}/{self.database}"
            self.engine = create_engine(connection_string)
            
            logger.info("Conexión a base de datos establecida correctamente")
            return True
            
        except Exception as e:
            logger.error(f"Error conectando a la base de datos: {e}")
            return False
    
    def insert_append(self, table_name, df):
        """Inserta datos usando append (sin reemplazar)"""
        try:
            if self.engine is None:
                logger.error("No hay conexión activa a la base de datos")
                return False
                
            # Insertar datos
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logger.info(f"Datos insertados correctamente en {table_name}: {len(df)} registros")
            return True
            
        except Exception as e:
            logger.error(f"Error insertando datos: {e}")
            return False
    
    def execute_query(self, query, params=None):
        """Ejecuta una consulta SQL"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()
        except Exception as e:
            logger.error(f"Error ejecutando consulta: {e}")
            return None
    
    def close_connections(self):
        """Cierra las conexiones"""
        try:
            if self.connection:
                self.connection.close()
                logger.info("Conexión pymysql cerrada")
                
            if self.engine:
                self.engine.dispose()
                logger.info("Engine SQLAlchemy cerrado")
                
        except Exception as e:
            logger.error(f"Error cerrando conexiones: {e}")