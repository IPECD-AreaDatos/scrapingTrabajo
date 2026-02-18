"""
LOAD - Módulo de carga de datos VentasCombustible
Responsabilidad: Cargar solo filas nuevas (por fecha) a MySQL
"""
import logging
import pandas as pd
from pymysql import connect
from sqlalchemy import create_engine, inspect

logger = logging.getLogger(__name__)

TABLA = 'combustible'


class LoadVentasCombustible:
    """Carga los datos de ventas de combustible a MySQL (append por fecha)."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._engine  = None
        self._conn    = None

    def load(self, df: pd.DataFrame) -> bool:
        """
        Carga solo filas con fecha posterior a la última en BD.

        Returns:
            bool: True si se cargaron datos nuevos
        """
        self._conn = connect(
            host=self.host, user=self.user,
            password=self.password, database=self.database
        )
        try:
            with self._conn.cursor() as cur:
                cur.execute(f"SELECT MAX(fecha) FROM {TABLA}")
                ultima_fecha_bdd = cur.fetchone()[0]
        finally:
            self._conn.close()

        df['fecha'] = pd.to_datetime(df['fecha'])

        if ultima_fecha_bdd is None:
            df_nuevos = df
        else:
            df_nuevos = df[df['fecha'] > pd.to_datetime(ultima_fecha_bdd)]

        if df_nuevos.empty:
            logger.info("[LOAD] Sin datos nuevos en '%s'.", TABLA)
            return False

        engine = self._get_engine()
        insp   = inspect(engine)
        if TABLA not in insp.get_table_names():
            df_nuevos.to_sql(name=TABLA, con=engine, if_exists='replace', index=False)
        else:
            df_nuevos.to_sql(name=TABLA, con=engine, if_exists='append', index=False)

        logger.info("[LOAD] %d filas nuevas en '%s'.", len(df_nuevos), TABLA)
        return True

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
