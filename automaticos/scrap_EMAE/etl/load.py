"""
LOAD - Módulo de carga de datos EMAE
Responsabilidad: Cargar las 2 tablas del EMAE a PostgreSQL (incremental)
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

# Recomendación: usa nombres en minúsculas en Postgres
TABLA_VALORES     = "emae"
TABLA_VARIACIONES = "emae_variaciones"

class LoadEMAE:
    """Carga los 2 DataFrames del EMAE a PostgreSQL."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.url = f"postgresql+psycopg2://{user}:{password}@{host}:5432/{database}"
        self._engine = create_engine(self.url)

    def load(self, df_valores: pd.DataFrame, df_variaciones: pd.DataFrame) -> bool:
        b1 = self._cargar_valores(df_valores)
        b2 = self._cargar_variaciones(df_variaciones)
        return b1 or b2

    def _cargar_valores(self, df: pd.DataFrame) -> bool:
        """Carga solo fechas nuevas en la tabla emae."""
        # En Postgres las columnas suelen estar en minúsculas
        df_bdd = pd.read_sql(f"SELECT fecha FROM {TABLA_VALORES}", con=self._engine)
        fechas_existentes = set(pd.to_datetime(df_bdd['fecha']).dt.strftime('%Y-%m-%d'))
        
        df_nuevos = df[~df['fecha'].isin(fechas_existentes)]
        
        if not df_nuevos.empty:
            df_nuevos.to_sql(name=TABLA_VALORES, con=self._engine, if_exists='append', index=False, method='multi')
            logger.info("[LOAD] EMAE valores: %d registros nuevos.", len(df_nuevos))
            return True
        logger.info("[LOAD] Sin datos nuevos en '%s'.", TABLA_VALORES)
        return False

    def _cargar_variaciones(self, df: pd.DataFrame) -> bool:
        """Carga solo filas nuevas en la tabla emae_variaciones."""
        with self._engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLA_VARIACIONES}"))
            len_bdd = result.scalar()
            
        len_df = len(df)
        logger.info("[LOAD] %s — BD: %d | DF: %d", TABLA_VARIACIONES, len_bdd, len_df)
        
        if len_df > len_bdd:
            df_tail = df.tail(len_df - len_bdd)
            df_tail.to_sql(name=TABLA_VARIACIONES, con=self._engine, if_exists='append', index=False, method='multi')
            logger.info("[LOAD] %d filas nuevas en '%s'.", len(df_tail), TABLA_VARIACIONES)
            return True
        logger.info("[LOAD] Sin datos nuevos en '%s'.", TABLA_VARIACIONES)
        return False

    def close(self):
        if self._engine:
            self._engine.dispose()