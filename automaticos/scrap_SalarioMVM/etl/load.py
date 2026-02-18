"""
LOAD - Módulo de carga de datos SalarioMVM
Responsabilidad: Cargar solo filas nuevas a MySQL (incremental)
"""
import logging
import pymysql
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TABLA = 'salario_mvm'
COLUMNAS = ['indice_tiempo', 'salario_minimo_vital_movil_mensual',
            'salario_minimo_vital_movil_diario', 'salario_minimo_vital_movil_hora']


class LoadSalarioMVM:
    """Carga los datos del Salario MVM a MySQL de forma incremental."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._engine  = None

    def load(self, df: pd.DataFrame) -> bool:
        """
        Carga solo las filas nuevas (por diferencia de conteo).

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

        if len_df > len_bdd:
            df_nuevos = df.tail(len_df - len_bdd)
            df_nuevos[COLUMNAS].to_sql(
                name=TABLA, con=self._get_engine(), if_exists='append', index=False
            )
            logger.info("[LOAD] %d filas nuevas en '%s'.", len(df_nuevos), TABLA)
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
