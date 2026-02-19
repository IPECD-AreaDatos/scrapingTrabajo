"""
LOAD - Módulo de carga de datos Índice de Salarios
Responsabilidad: Cargar solo filas nuevas a MySQL (append incremental)
"""
import logging
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TABLA = "salario" # Nuevo nombre de tabla

class LoadIndiceSalarios:
    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = 5432 # Puerto Postgres
        self._engine = None
        self._conn = None

    def _conectar_nativo(self):
        """Conexión directa para consultas de control"""
        return psycopg2.connect(
            host=self.host, user=self.user,
            password=self.password, database=self.database,
            port=self.port
        )

    def load(self, df: pd.DataFrame) -> bool:
        # 1. Verificar cantidad de filas en la nueva base
        conn = self._conectar_nativo()
        try:
            with conn.cursor() as cur:
                # Verificamos si la tabla existe primero para evitar errores
                cur.execute(f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{TABLA}'")
                if cur.fetchone()[0] > 0:
                    cur.execute(f"SELECT COUNT(*) FROM public.{TABLA}")
                    len_bdd = cur.fetchone()[0]
                else:
                    len_bdd = 0
        finally:
            conn.close()

        len_df = len(df)
        logger.info("[LOAD] %s - BD: %d filas | DF: %d filas", TABLA, len_bdd, len_df)

        if len_df > len_bdd:
            # Carga incremental de las filas que sobran
            df_nuevos = df.tail(len_df - len_bdd)
            df_nuevos.to_sql(name=TABLA, con=self._get_engine(), schema='public', if_exists='append', index=False)
            logger.info("[LOAD] %d filas nuevas cargadas en '%s'.", len(df_nuevos), TABLA)
            return True
        else:
            logger.info("[LOAD] No hay datos nuevos para la tabla %s.", TABLA)
            return False

    def _get_engine(self):
        if self._engine is None:
            self._engine = create_engine(
                f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            )
        return self._engine

    def close(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None