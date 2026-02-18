"""
LOAD - Módulo de carga de datos SRT
Responsabilidad: Cargar el DataFrame a PostgreSQL (append)
"""
import logging
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TABLA = "srt"


class LoadSRT:
    """Carga los datos del SRT a PostgreSQL."""

    def __init__(self, host: str, user: str, password: str, database: str, port: int = 5432):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self.port     = port
        self._engine  = None

    def load(self, df: pd.DataFrame):
        """Carga el DataFrame a la tabla srt (append)."""
        logger.info("[LOAD] Cargando %d filas en tabla '%s'...", len(df), TABLA)
        df.to_sql(name=TABLA, con=self._get_engine(), if_exists='append', index=False)
        logger.info("[LOAD] Carga completada.")

    def _get_engine(self):
        if self._engine is None:
            self._engine = create_engine(
                f"postgresql+psycopg2://{self.user}:{self.password}"
                f"@{self.host}:{self.port}/{self.database}"
            )
        return self._engine

    def close(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None
