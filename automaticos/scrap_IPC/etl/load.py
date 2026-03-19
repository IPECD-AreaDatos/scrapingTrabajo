"""
LOAD - Módulo de carga y reporte IPC
Responsabilidad: Cargar a MySQL y enviar correo de reporte
"""
import logging
import os
import calendar
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql
import psycopg2

logger = logging.getLogger(__name__)

class LoadIPC:
    
    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version)
        self.engine = None

    def conectar_bdd(self):
        if not self.engine:
            try:
                if self.version == "1":
                    puerto = int(self.port) if self.port else 3306
                    conn_str = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
                else:
                    puerto = int(self.port) if self.port else 5432
                    conn_str = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
                
                self.engine = create_engine(conn_str)
                logger.info(f"[OK] Conectado a BD v{self.version} ({self.host})")
            except Exception as e:
                logger.error(f"[ERROR] Conexión fallida v{self.version}: {e}")
                raise

    def _get_schema_prefix(self):
        return "public." if self.version == "2" else ""
    
    def load_to_db(self, df):
        """Carga los datos del IPC limpiando el rango de fechas actual"""
        self.conectar_bdd()
        
        # Aseguramos que los nombres coincidan con la imagen de tu DB
        columnas_db = [
            'fecha', 'id_region', 'id_categoria', 'id_division', 
            'id_subdivision', 'valor', 'var_mensual', 
            'var_interanual', 'var_acumulada'
        ]
        df_load = df[columnas_db].copy()
        
        tabla = f"{self._get_schema_prefix()}ipc"
        
        try:
            fecha_min = df_load['fecha'].min()
            fecha_max = df_load['fecha'].max()
            
            # Borramos para evitar duplicados en el rango procesado
            sql_delete = text(f"DELETE FROM {tabla} WHERE fecha BETWEEN :fmin AND :fmax")
            
            with self.engine.begin() as conn:
                conn.execute(sql_delete, {"fmin": fecha_min, "fmax": fecha_max})
                
                # Inserción (usamos 'ipc' como nombre base, SQLAlchemy maneja el schema)
                df_load.to_sql(name='ipc', con=conn, schema="public" if self.version == "2" else None,
                             if_exists='append', index=False, method='multi')
            
            logger.info(f"[LOAD] v{self.version} OK: {len(df_load)} filas en tabla 'ipc'.")
            return True
        except Exception as e:
            logger.error(f"[LOAD ERROR] v{self.version}: {e}")
            raise

    def cerrar_conexion(self):
        if self.engine:
            self.engine.dispose()