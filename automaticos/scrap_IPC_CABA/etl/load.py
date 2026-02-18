"""
LOAD - Módulo de carga de datos IPC CABA
Responsabilidad: Cargar el DataFrame a MySQL (TRUNCATE + append)
"""
import logging
import pandas as pd
import pymysql
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TABLA = "ipc_caba"


class LoadIPCCABA:
    """Carga los datos del IPC CABA a MySQL."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._engine  = None

    def load(self, df: pd.DataFrame):
        """Trunca la tabla y carga el DataFrame completo."""
        logger.info("[LOAD] Truncando tabla '%s'...", TABLA)
        conn = pymysql.connect(
            host=self.host, user=self.user,
            password=self.password, database=self.database
        )
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE {TABLA}")
        conn.commit()
        conn.close()

        logger.info("[LOAD] Cargando %d filas en '%s'...", len(df), TABLA)
        df.to_sql(name=TABLA, con=self._get_engine(), if_exists='append', index=False)
        logger.info("[LOAD] Carga completada.")

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
