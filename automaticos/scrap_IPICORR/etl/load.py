"""
LOAD - Módulo de carga de datos IPICORR
Responsabilidad: Cargar solo filas nuevas a MySQL (incremental)
"""
import logging
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

TABLA = "ipicorr"

class LoadIPICORR:
    def __init__(self, host: str, user: str, password: str, database: str):
        self.host     = host
        self.user     = user
        self.password = password
        self.database = database
        self.port     = 5432 # CAMBIO: Puerto Postgres
        self._engine  = None

    def load(self, df: pd.DataFrame) -> bool:
        # 1. Conexión nativa para verificar la base
        conn = psycopg2.connect(
            host=self.host, user=self.user,
            password=self.password, database=self.database, port=self.port
        )
        try:
            with conn.cursor() as cur:
                # Verificar si la tabla existe en Postgres
                cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{TABLA}')")
                existe = cur.fetchone()[0]
                
                if existe:
                    cur.execute(f"SELECT COUNT(*) FROM public.{TABLA}")
                    len_bdd = cur.fetchone()[0]
                else:
                    len_bdd = 0
                    logger.info(f"Tabla '{TABLA}' no existe. Se creará en la primera carga.")
        finally:
            conn.close()

        len_df = len(df)
        logger.info("[LOAD] IPICORR - BD: %d filas | DF: %d filas", len_bdd, len_df)

        if len_df > len_bdd:
            # Seleccionamos solo las filas nuevas basándonos en la diferencia de cantidad
            df_nuevos = df.tail(len_df - len_bdd)
            
            # Aseguramos formato de fecha antes de subir
            df_nuevos['fecha'] = pd.to_datetime(df_nuevos['fecha']).dt.date

            df_nuevos.to_sql(
                name=TABLA, 
                con=self._get_engine(), 
                schema='public', 
                if_exists='append', 
                index=False
            )
            logger.info("[LOAD] %d filas nuevas cargadas en PostgreSQL.", len(df_nuevos))
            return True
        else:
            logger.info("[LOAD] No hay datos nuevos para IPICORR.")
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