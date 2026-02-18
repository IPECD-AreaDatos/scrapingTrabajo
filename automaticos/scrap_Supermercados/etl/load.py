"""
LOAD - Módulo de carga de datos Supermercados
Responsabilidad: Cargar datos si hay novedades (incremental por conteo de filas)
"""
import logging
import pandas as pd
import pymysql
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TABLA = "supermercado_encuesta"


class LoadSupermercados:
    """Carga los datos de supermercados a MySQL (incremental)."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._engine  = None

    def load(self, df: pd.DataFrame) -> bool:
        """
        Carga el DataFrame si tiene más filas que la BD (TRUNCATE + append).

        Returns:
            bool: True si se cargaron datos nuevos
        """
        conn = pymysql.connect(
            host=self.host, user=self.user,
            password=self.password, database=self.database
        )
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLA}")
            len_bdd = cur.fetchone()[0]
            if len(df) > len_bdd:
                cur.execute(f"TRUNCATE {TABLA}")
                conn.commit()
        conn.close()

        if len(df) > len_bdd:
            df.to_sql(name=TABLA, con=self._get_engine(), if_exists='append', index=False)
            logger.info("[LOAD] '%s' actualizada: %d filas.", TABLA, len(df))
            return True
        else:
            logger.info("[LOAD] No hay datos nuevos de supermercados.")
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
