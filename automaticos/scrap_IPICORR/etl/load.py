"""
LOAD - Módulo de carga de datos IPICORR
Responsabilidad: Cargar solo filas nuevas a MySQL (incremental)
"""
import logging
import pandas as pd
import pymysql
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TABLA = "ipicorr"
COLUMNAS_INSERT = [
    'Fecha', 'Var_ia_Nivel_General', 'Vim_Nivel_General', 'Vim_Alimentos',
    'Vim_Textil', 'Vim_Maderas', 'Vim_min_nometalicos', 'Vim_metales',
    'Var_ia_Alimentos', 'Var_ia_Textil', 'Var_ia_Maderas',
    'Var_ia_min_nometalicos', 'Var_ia_metales'
]


class LoadIPICORR:
    """Carga los datos del IPICORR a MySQL de forma incremental."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._engine  = None

    def load(self, df: pd.DataFrame) -> bool:
        """
        Carga solo las filas nuevas si el DF tiene más datos que la BD.

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
        conn.close()

        len_df = len(df)
        logger.info("[LOAD] BD: %d filas | DF: %d filas", len_bdd, len_df)

        if len_df != len_bdd:
            df_nuevos = df.tail(len_df - len_bdd)
            df_nuevos[COLUMNAS_INSERT].to_sql(
                name=TABLA, con=self._get_engine(), if_exists='append', index=False
            )
            logger.info("[LOAD] %d filas nuevas cargadas en '%s'.", len(df_nuevos), TABLA)
            return True
        else:
            logger.info("[LOAD] No hay datos nuevos de IPICORR.")
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
