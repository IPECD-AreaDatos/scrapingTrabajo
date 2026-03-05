"""
LOAD - Módulo de carga de datos OEDE
Responsabilidad: Cargar solo filas nuevas a PostgreSQL
"""
import logging
import pandas as pd
import psycopg2
import pymysql
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

TABLA = "oede" 

class LoadOEDE:
    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version)
        self.engine = None
        self.tabla = "oede"

    def _conectar(self):
        if self.engine is None:
            if self.version == "1":  # MySQL
                puerto = int(self.port) if self.port else 3306
                url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            else:  # PostgreSQL
                puerto = int(self.port) if self.port else 5432
                url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            
            self.engine = create_engine(url, echo=False)
            logger.info(f"[OK] Motor conectado a OEDE (v{self.version})")

    def load(self, df: pd.DataFrame) -> bool:
        self._conectar()
        schema = "public" if self.version == "2" else None
        full_table = f"{schema}.{self.tabla}" if schema else self.tabla
        
        # 1. Asegurar formato fecha
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date
        
        # 2. Estrategia UPSERT (Delete por fecha antes de Insert)
        # Esto es mucho más seguro que medir el largo del DF
        with self.engine.begin() as conn:
            # Identificamos qué meses vienen en el DF nuevo
            fechas_a_actualizar = tuple(df['fecha'].unique().tolist())
            
            # Borramos los registros que coincidan con esas fechas para reemplazarlos
            if fechas_a_actualizar:
                conn.execute(text(f"DELETE FROM {full_table} WHERE fecha IN :fechas"), 
                             {"fechas": fechas_a_actualizar})
            
            # 3. Carga limpia
            df.to_sql(
                name=self.tabla, 
                con=conn, 
                schema=schema, 
                if_exists='append', 
                index=False, 
                method='multi'
            )
            
        logger.info(f"[LOAD] {len(df)} registros procesados en '{self.tabla}'.")
        return True

    def close(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None