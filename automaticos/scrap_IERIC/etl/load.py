"""
LOAD - Módulo de carga de datos IERIC
Responsabilidad: Cargar las 3 tablas del IERIC a MySQL (REPLACE)
"""
import logging
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TABLAS = {
    'actividad': 'ieric_actividad',
    'puestos':   'ieric_puestos_trabajo',
    'salario':   'ieric_ingreso',
}


class LoadIERIC:
    """Carga los 3 DataFrames del IERIC a MySQL (replace completo)."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._engine  = None

    def load(self, df_actividad: pd.DataFrame, df_puestos: pd.DataFrame,
             df_salario: pd.DataFrame):
        """Carga las 3 tablas con replace completo."""
        self._cargar(df_actividad, TABLAS['actividad'])
        self._cargar(df_puestos,   TABLAS['puestos'])
        self._cargar(df_salario,   TABLAS['salario'])

    def _cargar(self, df: pd.DataFrame, tabla: str):
        df.to_sql(name=tabla, con=self._get_engine(), if_exists='replace', index=False)
        logger.info("[LOAD] '%s' actualizada: %d filas.", tabla, len(df))

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
