"""
LOAD - Módulo de carga de datos EMAE
Responsabilidad: Cargar las 2 tablas del EMAE a MySQL (incremental)
"""
import logging
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TABLA_VALORES     = "emae"
TABLA_VARIACIONES = "emae_variaciones"


class LoadEMAE:
    """Carga los 2 DataFrames del EMAE a MySQL."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._engine  = None

    def load(self, df_valores: pd.DataFrame, df_variaciones: pd.DataFrame) -> bool:
        """
        Carga ambas tablas del EMAE de forma incremental.

        Returns:
            bool: True si se cargó al menos una tabla nueva
        """
        b1 = self._cargar_valores(df_valores)
        b2 = self._cargar_variaciones(df_variaciones)
        return b1 or b2

    def _cargar_valores(self, df: pd.DataFrame) -> bool:
        """Carga solo fechas nuevas en la tabla emae."""
        df_bdd = pd.read_sql(f"SELECT fecha FROM {TABLA_VALORES}", con=self._get_engine())
        fechas_existentes = set(df_bdd['fecha'])
        df_nuevos = df[~df['fecha'].isin(fechas_existentes)]
        if not df_nuevos.empty:
            df_nuevos.to_sql(name=TABLA_VALORES, con=self._get_engine(), if_exists='append', index=False)
            logger.info("[LOAD] EMAE valores: %d registros nuevos.", len(df_nuevos))
            return True
        logger.info("[LOAD] Sin datos nuevos en '%s'.", TABLA_VALORES)
        return False

    def _cargar_variaciones(self, df: pd.DataFrame) -> bool:
        """Carga solo filas nuevas en la tabla emae_variaciones."""
        df_bdd = pd.read_sql(f"SELECT COUNT(*) as cnt FROM {TABLA_VARIACIONES}", con=self._get_engine())
        len_bdd = df_bdd['cnt'].iloc[0]
        len_df  = len(df)
        logger.info("[LOAD] %s — BD: %d | DF: %d", TABLA_VARIACIONES, len_bdd, len_df)
        if len_df > len_bdd:
            df_tail = df.tail(len_df - len_bdd)
            df_tail.to_sql(name=TABLA_VARIACIONES, con=self._get_engine(), if_exists='append', index=False)
            logger.info("[LOAD] %d filas nuevas en '%s'.", len(df_tail), TABLA_VARIACIONES)
            return True
        logger.info("[LOAD] Sin datos nuevos en '%s'.", TABLA_VARIACIONES)
        return False

    def _get_engine(self):
        if self._engine is None:
            self._engine = create_engine(
                f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}"
            )
        return self._engine

    def close(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None
