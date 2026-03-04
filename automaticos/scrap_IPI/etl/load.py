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


class LoadIPI:
    """Carga los 3 DataFrames del IPI a MySQL de forma incremental."""

    TABLAS = {
        'valores': 'ipi_valores',
        'variaciones': 'ipi_variacion_interanual',
        'acum': 'ipi_var_interacum',
    }

    def __init__(self, host, user, password, db_datalake, db_dwh, port, version="2"):
        self.host, self.user, self.pwd, self.port = host, user, password, port
        self.db_datalake = db_datalake
        self.db_dwh = db_dwh
        self.version = version
        self.engines = {}

    def _get_engine(self, db_name):
        if db_name not in self.engines:
            url = f"postgresql+psycopg2://{self.user}:{self.pwd}@{self.host}:{self.port}/{db_name}"
            self.engines[db_name] = create_engine(url)
        return self.engines[db_name]

    def load(self, dfs_dict: dict):
        self._conectar()
        
        # 1. Definir destinos según versión
        if self.version == "1":
            # En v1 (MySQL), todo va a la misma base (datalake)
            destinos = {
                'ipi_valores': self.db_datalake,
                'ipi_variacion_interanual': self.db_datalake,
                'ipi_var_interacum': self.db_datalake
            }
        else:
            # En v2 (Postgres), separamos como pediste
            destinos = {
                'ipi_valores': self.db_datalake,
                'ipi_variacion_interanual': self.db_dwh,
                'ipi_var_interacum': self.db_dwh
            }

        # 2. Iterar y cargar
        for tabla, db_name in destinos.items():
            df = dfs_dict[tabla]
            engine = self._get_engine(db_name)
            
            # Ajuste de esquema para Postgres
            schema = 'public' if self.version == "2" else None
            
            logger.info(f"[LOAD] Cargando tabla {tabla} en base {db_name}...")
            
            # Borrado y carga (Estrategia segura para evitar duplicados)
            with engine.begin() as conn:
                # Si es MySQL no usamos schema
                full_table = f"{schema}.{tabla}" if schema else tabla
                
                # Borrar datos de las fechas que vienen en el DF para asegurar "upsert"
                fechas = tuple(pd.to_datetime(df['fecha']).dt.date.tolist())
                conn.execute(text(f"DELETE FROM {full_table} WHERE fecha IN :fechas"), {"fechas": fechas})
                
                # Cargar
                df.to_sql(tabla, engine, schema=schema, if_exists='append', index=False, method='multi')
                
        logger.info("[OK] Las 3 tablas del IPI fueron cargadas exitosamente.")

    def close(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None
