"""
LOAD - Módulo de carga de datos SalarioSPTotal
Responsabilidad: Cargar los 2 DataFrames a MySQL (incremental)
"""
import logging
import pandas as pd
import pymysql
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


class LoadSalarioSPTotal:
    """Carga los 2 DataFrames de salarios a MySQL de forma incremental."""

    TABLA_SP    = "dp_salarios_sector_privado"
    TABLA_TOTAL = "dp_salarios_total"

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._engine  = None

    def load(self, df_sp: pd.DataFrame, df_total: pd.DataFrame):
        """Carga ambas tablas de forma incremental."""
        self._cargar(df_sp,    self.TABLA_SP)
        self._cargar(df_total, self.TABLA_TOTAL)

    def _cargar(self, df: pd.DataFrame, tabla: str) -> bool:
        conn = pymysql.connect(
            host=self.host, user=self.user,
            password=self.password, database=self.database
        )
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {tabla}")
            len_bdd = cur.fetchone()[0]
        conn.close()

        len_df = len(df)
        logger.info("[LOAD] %s — BD: %d | DF: %d", tabla, len_bdd, len_df)

        if len_df > len_bdd:
            df_nuevos = df.tail(len_df - len_bdd)
            df_nuevos.to_sql(name=tabla, con=self._get_engine(), if_exists='append', index=False)
            logger.info("[LOAD] %d filas nuevas en '%s'.", len(df_nuevos), tabla)
            return True
        logger.info("[LOAD] Sin datos nuevos en '%s'.", tabla)
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
