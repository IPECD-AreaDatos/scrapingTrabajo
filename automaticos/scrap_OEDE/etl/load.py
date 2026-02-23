"""
LOAD - Módulo de carga de datos OEDE
Responsabilidad: Cargar solo filas nuevas a PostgreSQL
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

TABLA = "oede" 

class LoadOEDE:
    def __init__(self, host: str, user: str, password: str, database: str):
        self.url = f"postgresql+psycopg2://{user}:{password}@{host}:5432/{database}"
        self._engine = create_engine(self.url)

    def load(self, df: pd.DataFrame) -> bool:
        # Verificamos cantidad de filas actuales
        with self._engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLA}"))
            len_bdd = result.scalar()

        len_df = len(df)
        logger.info("[LOAD] BD: %d filas | DF: %d filas", len_bdd, len_df)

        if len_df > len_bdd:
            # Seleccionamos solo las filas nuevas (cola del dataframe)
            df_nuevos = df.tail(len_df - len_bdd)
            
            # Cargamos a Postgres
            df_nuevos.to_sql(
                name=TABLA, 
                con=self._engine, 
                if_exists='append', 
                index=False,
                method='multi' # Mejora velocidad en Postgres
            )
            logger.info("[LOAD] %d filas nuevas cargadas.", len(df_nuevos))
            return True
        
        logger.info("[LOAD] No hay datos nuevos.")
        return False

    def close(self):
        if self._engine:
            self._engine.dispose()