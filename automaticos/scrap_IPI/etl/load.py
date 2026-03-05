"""
LOAD - Módulo de carga de datos IPI
Responsabilidad: Cargar las 3 tablas del IPI a MySQL (incremental)
"""
import os
import logging
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from google.oauth2 import service_account
from googleapiclient.discovery import build
from json import loads

logger = logging.getLogger(__name__)

logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

class LoadIPI:
    """Carga los DataFrames del IPI a PostgreSQL/MySQL de forma incremental."""

    def __init__(self, host, user, password, db_datalake, db_dwh, port, version="2"):
        self.host = host
        self.user = user
        self.password = password
        self.db_datalake = db_datalake
        self.db_dwh = db_dwh
        self.port = port
        self.version = str(version)
        self.engines = {}

    def _get_engine(self, db_name):
        """Crea o retorna el motor de conexión para una base de datos específica."""
        if db_name not in self.engines:
            if self.version == "1":  # MySQL
                port = int(self.port) if self.port else 3306
                url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{port}/{db_name}"
            else:  # PostgreSQL
                port = int(self.port) if self.port else 5432
                url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{port}/{db_name}"
            
            self.engines[db_name] = create_engine(url, echo=False)
            logger.info(f"[OK] Motor conectado a base '{db_name}' (v{self.version})")
        return self.engines[db_name]

    def _get_schema(self):
        return "public" if self.version == "2" else None

    def load(self, dfs_dict: dict):
        """Carga el diccionario de DataFrames en sus respectivas bases de datos."""
        
        # 1. Definir mapeo: clave del dict -> (base_datos, nombre_tabla_bdd)
        if self.version == "1":
            # Todo va al datalake
            configuracion = {
                'valores': (self.db_datalake, 'ipi'),
                'variaciones': (self.db_datalake, 'ipi_variacion_interanual'),
                'acumulado': (self.db_datalake, 'ipi_variacion_interacumulada')
            }
        else:
            # v2: Separación entre datalake y dwh
            configuracion = {
                'valores': (self.db_datalake, 'ipi'),
                'variaciones': (self.db_dwh, 'ipi_variacion_interanual'),
                'acumulado': (self.db_dwh, 'ipi_variacion_interacumulada')
            }

        schema = self._get_schema()

        # 2. Iterar sobre cada tabla
        for key, (db_name, tabla) in configuracion.items():
            df = dfs_dict[key]
            engine = self._get_engine(db_name)
            
            logger.info(f"[LOAD] Cargando {key} en base {db_name}...")
            
            with engine.begin() as conn:
                full_table = f"{schema}.{tabla}" if schema else tabla
                
                # Borrado seguro por fechas para evitar duplicados
                fechas = tuple(pd.to_datetime(df['fecha']).dt.date.tolist())
                conn.execute(text(f"DELETE FROM {full_table} WHERE fecha IN :fechas"), {"fechas": fechas})
                
                # Carga
                df.to_sql(tabla, conn, schema=schema, if_exists='append', index=False, method='multi')
                
        logger.info("[OK] Las 3 tablas del IPI fueron cargadas exitosamente.")

    def close(self):
        """Cierra todos los motores de conexión abiertos."""
        for db_name, engine in self.engines.items():
            engine.dispose()
            logger.info(f"[LOAD] Conexión a '{db_name}' cerrada.")
        self.engines = {}