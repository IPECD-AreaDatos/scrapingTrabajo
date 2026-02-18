"""
LOAD - Módulo de carga de datos Índice de Salarios
Responsabilidad: Cargar solo filas nuevas a MySQL (append incremental)
"""
import logging
import pandas as pd
import pymysql
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TABLA = "indicadores_salarios"


class LoadIndiceSalarios:
    """Carga incrementalmente los datos del Índice de Salarios a MySQL."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._engine  = None

    def load(self, df: pd.DataFrame) -> bool:
        """
        Carga solo las filas nuevas (si df tiene más filas que la BD).

        Returns:
            bool: True si se cargaron datos nuevos, False si no había novedades
        """
        conn = pymysql.connect(
            host=self.host, user=self.user,
            password=self.password, database=self.database
        )
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLA}")
            len_bdd = cur.fetchone()[0]
        conn.close()

        len_df = len(df)
        logger.info("[LOAD] BD: %d filas | DF: %d filas", len_bdd, len_df)

        if len_df > len_bdd:
            df_nuevos = df.tail(len_df - len_bdd)
            df_nuevos.to_sql(name=TABLA, con=self._get_engine(), if_exists='append', index=False)
            logger.info("[LOAD] %d filas nuevas cargadas en '%s'.", len(df_nuevos), TABLA)
            return True
        else:
            logger.info("[LOAD] No hay datos nuevos de Índice de Salarios.")
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
