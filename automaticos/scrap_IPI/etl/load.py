"""
LOAD - Módulo de carga de datos IPI
Responsabilidad: Cargar las 3 tablas del IPI a MySQL (incremental)
"""
import logging
import pandas as pd
import pymysql
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


class LoadIPI:
    """Carga los 3 DataFrames del IPI a MySQL de forma incremental."""

    TABLAS = {
        'valores': 'ipi_valores',
        'variaciones': 'ipi_variacion_interanual',
        'acum': 'ipi_var_interacum',
    }

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._engine  = None
        self._conn    = None

    def load(self, df_valores: pd.DataFrame, df_variaciones: pd.DataFrame,
             df_var_inter_acum: pd.DataFrame) -> bool:
        """
        Carga las 3 tablas del IPI de forma incremental.

        Returns:
            bool: True si se cargó al menos una tabla nueva
        """
        self._conn = pymysql.connect(
            host=self.host, user=self.user,
            password=self.password, database=self.database
        )
        try:
            b1 = self._cargar(df_valores,       self.TABLAS['valores'])
            b2 = self._cargar(df_variaciones,   self.TABLAS['variaciones'])
            b3 = self._cargar(df_var_inter_acum, self.TABLAS['acum'])
            self._conn.commit()
        finally:
            self._conn.close()
        return b1 or b2 or b3

    def _cargar(self, df: pd.DataFrame, tabla: str) -> bool:
        with self._conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {tabla}")
            len_bdd = cur.fetchone()[0]
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
