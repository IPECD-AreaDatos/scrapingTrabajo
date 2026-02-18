"""
LOAD - Módulo de carga de datos REM
Responsabilidad: Cargar precios minoristas y cambio nominal a MySQL
"""
import logging
import pandas as pd
import pymysql
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TABLA_PRECIOS = "rem_precios_minoristas"
TABLA_CAMBIO  = "rem_cambio_nominal"
TABLA_IPC_REM = "ipc_rem_variaciones"


class LoadREM:
    """Carga los DataFrames del REM a MySQL."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._engine  = None

    def load(self, df_precios: pd.DataFrame, df_cambio: pd.DataFrame):
        """Carga ambas tablas del REM."""
        self._cargar_precios_minoristas(df_precios)
        self._cargar_cambio_nominal(df_cambio)

    def _cargar_precios_minoristas(self, df: pd.DataFrame):
        """Trunca y recarga la tabla de precios minoristas."""
        self._truncar(TABLA_PRECIOS)
        df.to_sql(name=TABLA_PRECIOS, con=self._get_engine(), if_exists='replace', index=False)
        logger.info("[LOAD] '%s' actualizada: %d filas.", TABLA_PRECIOS, len(df))

    def _cargar_cambio_nominal(self, df: pd.DataFrame):
        """Trunca y recarga la tabla de cambio nominal."""
        self._truncar(TABLA_CAMBIO)
        df.to_sql(name=TABLA_CAMBIO, con=self._get_engine(), if_exists='replace', index=False)
        logger.info("[LOAD] '%s' actualizada: %d filas.", TABLA_CAMBIO, len(df))

    def _truncar(self, tabla: str):
        conn = pymysql.connect(
            host=self.host, user=self.user,
            password=self.password, database=self.database
        )
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {tabla}")
        conn.commit()
        conn.close()

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
