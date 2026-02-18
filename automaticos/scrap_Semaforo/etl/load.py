"""
LOAD - Módulo de carga de datos Semáforo
Responsabilidad: Cargar los DataFrames transformados a MySQL
"""
import logging
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


class LoadSemaforo:
    """Carga los datos del Semáforo a la base de datos MySQL."""

    TABLA_INTERANUAL   = "semaforo_interanual"
    TABLA_INTERMENSUAL = "semaforo_intermensual"

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._engine  = None

    def load(self, df_interanual: pd.DataFrame, df_intermensual: pd.DataFrame):
        """
        Carga ambos DataFrames a MySQL (replace completo).

        Args:
            df_interanual: DataFrame con variaciones interanuales
            df_intermensual: DataFrame con variaciones intermensuales
        """
        engine = self._get_engine()
        self._cargar_tabla(df_interanual,   self.TABLA_INTERANUAL,   engine)
        self._cargar_tabla(df_intermensual, self.TABLA_INTERMENSUAL, engine)

    def _cargar_tabla(self, df: pd.DataFrame, tabla: str, engine):
        """Carga un DataFrame a una tabla MySQL (replace)."""
        logger.info("[LOAD] Cargando %d filas en tabla '%s'...", len(df), tabla)
        df.to_sql(name=tabla, con=engine, if_exists='replace', index=False)
        logger.info("[LOAD] Tabla '%s' actualizada correctamente.", tabla)

    def _get_engine(self):
        if self._engine is None:
            conn_str = (
                f"mysql+pymysql://{self.user}:{self.password}"
                f"@{self.host}:3306/{self.database}"
            )
            self._engine = create_engine(conn_str)
        return self._engine

    def close(self):
        """Cierra el engine de SQLAlchemy."""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            logger.info("[LOAD] Conexión a BD cerrada.")
