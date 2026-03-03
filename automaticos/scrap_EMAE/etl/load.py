"""
LOAD - Módulo de carga de datos EMAE
Responsabilidad: Cargar las 2 tablas del EMAE a PostgreSQL (incremental)
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql
import psycopg2

logger = logging.getLogger(__name__)

logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

# Recomendación: usa nombres en minúsculas en Postgres
TABLA_VALORES     = "emae"
TABLA_VARIACIONES = "emae_variaciones"

class LoadEMAE:
    """Carga los 2 DataFrames del EMAE a PostgreSQL."""

    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = version
        self.engine = None

    def _conectar(self):
        if not self.engine:
            if self.version == "1":  # MySQL
                puerto = int(self.port) if self.port else 3306
                url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            else:  # PostgreSQL
                puerto = int(self.port) if self.port else 5432
                url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            
            self.engine = create_engine(url, echo=False)
            logger.info(f"[OK] Motor conectado a {'MySQL' if self.version=='1' else 'PostgreSQL'} (v{self.version})")

    def _get_schema(self):
        return "public" if self.version == "2" else None

    def load(self, df_valores: pd.DataFrame, df_variaciones: pd.DataFrame) -> bool:
        self._conectar()
        b1 = self._cargar_valores(df_valores)
        b2 = self._cargar_variaciones(df_variaciones)
        return b1 or b2

    def _cargar_valores(self, df: pd.DataFrame) -> bool:
        """Carga solo fechas nuevas en la tabla emae."""
        # En Postgres las columnas suelen estar en minúsculas

        tabla = "emae"
        schema = self._get_schema()

        # Leemos datos existentes para evitar duplicados por fecha
        try:
            # pd.read_sql maneja internamente el esquema si pasamos el engine
            query = f"SELECT fecha FROM {tabla}" if not schema else f"SELECT fecha FROM {schema}.{tabla}"
            df_bdd = pd.read_sql(query, con=self.engine)
            fechas_existentes = set(pd.to_datetime(df_bdd['fecha']).dt.strftime('%Y-%m-%d'))
        except Exception:
            fechas_existentes = set()
        
        df_nuevos = df[~df['fecha'].dt.strftime('%Y-%m-%d').isin(fechas_existentes)]
        
        if not df_nuevos.empty:
            df_nuevos.to_sql(name=tabla, con=self.engine, schema=schema, if_exists='append', index=False, method='multi')
            logger.info("[LOAD] EMAE valores: %d registros cargados.", len(df_nuevos))
            return True
        return False

    def _cargar_variaciones(self, df: pd.DataFrame) -> bool:
        tabla = "emae_variaciones"
        schema = self._get_schema()
        
        full_table_name = f"{schema}.{tabla}" if schema else tabla
        
        with self.engine.connect() as conn:
            # Usamos text() para consultas SQL crudas
            result = conn.execute(text(f"SELECT COUNT(*) FROM {full_table_name}"))
            len_bdd = result.scalar()
            
        if len(df) > len_bdd:
            df_tail = df.tail(len(df) - len_bdd)
            df_tail.to_sql(name=tabla, con=self.engine, schema=schema, if_exists='append', index=False, method='multi')
            logger.info("[LOAD] EMAE variaciones: %d registros cargados.", len(df_tail))
            return True
        return False

    def close(self):
        if self.engine:
            self.engine.dispose()