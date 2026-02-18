"""
LOAD - Módulo de carga de datos DNRPA
Responsabilidad: Cargar datos del último año (DELETE + INSERT para el año en cuestión)
"""
import logging
import pymysql
import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TABLA = 'dnrpa'


class LoadDNRPA:
    """Carga los datos del DNRPA a MySQL (DELETE del año + INSERT)."""

    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self._engine  = None

    def load(self, df: pd.DataFrame) -> bool:
        """
        Compara el conteo del año con la BD y recarga si hay diferencias.

        Returns:
            bool: True si se cargaron datos nuevos
        """
        df = df.sort_values(by='fecha', ascending=True)
        ultimo_anio = df['fecha'].dt.year.max()
        cantidad_df = len(df[df['fecha'].dt.year == ultimo_anio])

        conn = pymysql.connect(
            host=self.host, user=self.user,
            password=self.password, database=self.database
        )
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {TABLA} WHERE YEAR(fecha) = %s", (ultimo_anio,))
            cantidad_tabla = cur.fetchone()[0]

        logger.info("[LOAD] DNRPA %d — BD: %d | DF: %d", ultimo_anio, cantidad_tabla, cantidad_df)

        if cantidad_tabla == cantidad_df:
            logger.info("[LOAD] Sin datos nuevos para DNRPA %d.", ultimo_anio)
            conn.close()
            return False

        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TABLA} WHERE YEAR(fecha) = %s", (ultimo_anio,))
        conn.commit()
        conn.close()

        df_nuevo = df[df['fecha'].dt.year == ultimo_anio]
        df_nuevo.to_sql(name=TABLA, con=self._get_engine(), if_exists='append', index=False)
        logger.info("[LOAD] DNRPA %d recargado: %d filas.", ultimo_anio, len(df_nuevo))
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
