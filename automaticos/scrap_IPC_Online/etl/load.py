"""
LOAD - Módulo de carga de datos IPC_Online
Responsabilidad: Cargar solo filas nuevas (por fecha) a MySQL
"""
import logging
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TABLA = 'ipc_online'


class LoadIPCOnline:
    """Carga los datos del IPC Online a MySQL (append por fecha)."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._engine  = None

    def load(self, df: pd.DataFrame) -> bool:
        """
        Carga solo filas cuya fecha no existe en la tabla.

        Returns:
            bool: True si se cargaron datos nuevos
        """
        engine = self._get_engine()
        df_existente = pd.read_sql(f"SELECT fecha FROM {TABLA}", con=engine)
        nuevas = df[~df['fecha'].isin(df_existente['fecha'])]

        if not nuevas.empty:
            nuevas.to_sql(name=TABLA, con=engine, if_exists='append', index=False)
            logger.info("[LOAD] %d filas nuevas en '%s'.", len(nuevas), TABLA)
            return True
        logger.info("[LOAD] Sin datos nuevos en '%s'.", TABLA)
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
