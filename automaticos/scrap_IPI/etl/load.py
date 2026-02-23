"""
LOAD - Módulo de carga de datos IPI
Responsabilidad: Cargar las 3 tablas del IPI a MySQL (incremental)
"""
import os
import logging
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
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

    def __init__(self, host, user, password, database):
        self.host, self.user, self.password, self.database = host, user, password, database
        self.port = 5432
        self.engine = None
        self.conn = None

    def _conectar(self):
        if not self.conn:
            self.conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, database=self.database, port=self.port)
            url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            self.engine = create_engine(url)

    def load(self, df: pd.DataFrame) -> bool:
        try:
            self._conectar()
            cursor = self.conn.cursor()

            # 1. Verificar fecha máxima en BD
            cursor.execute("SELECT MAX(fecha) FROM public.ipi")
            ultima_fecha_bd = cursor.fetchone()[0]

            df['fecha'] = pd.to_datetime(df['fecha']).dt.date
            if ultima_fecha_bd:
                df_nuevos = df[df['fecha'] > ultima_fecha_bd]
            else:
                df_nuevos = df

            if df_nuevos.empty:
                logger.info("[LOAD] IPI: No hay datos nuevos.")
                return False

            # 2. Carga a Postgres
            df_nuevos.to_sql('ipi', self.engine, schema='public', if_exists='append', index=False)
            logger.info(f"[OK] {len(df_nuevos)} meses nuevos cargados en Postgres.")
            return True
        except Exception as e:
            logger.error(f"Error cargando IPI: {e}")
            raise

    def close(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None
