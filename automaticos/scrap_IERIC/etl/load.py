"""
LOAD - Módulo de carga de datos IERIC
Responsabilidad: Cargar las 3 tablas del IERIC a MySQL (REPLACE)
"""
import logging
import pandas as pd
from sqlalchemy import create_engine,text

logger = logging.getLogger(__name__)

TABLAS = {
    'actividad': 'ieric_actividad',
    'puestos':   'ieric_puestos_trabajo',
    'salario':   'ieric_salario',
}


class LoadIERIC:
    """Carga los 3 DataFrames del IERIC a MySQL (replace completo)."""

    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version)
        self._engine = None

    def load(self, df_act, df_puestos, df_sal):
        """Carga las 3 tablas. Usa 'replace' para v1 y lógica de limpieza para v2."""
        self._cargar(df_act, TABLAS['actividad'])
        self._cargar(df_puestos, TABLAS['puestos'])
        self._cargar(df_sal, TABLAS['salario'])

    def _cargar(self, df, tabla_nombre):
        # En Postgres (v2) usamos esquema public.
        schema = "public" if self.version == "2" else None
        
        try:
            # Para mantener consistencia con tus otros scripts, usamos 'replace' 
            # para asegurar que los datos estén frescos.
            df.to_sql(
                name=tabla_nombre, 
                con=self._get_engine(), 
                schema=schema, 
                if_exists='replace', 
                index=False,
                method='multi' if self.version == "2" else None
            )
            logger.info("[LOAD] v%s - Tabla '%s' actualizada: %d filas.", self.version, tabla_nombre, len(df))
        except Exception as e:
            logger.error("[LOAD ERROR] v%s en tabla %s: %s", self.version, tabla_nombre, e)
            raise

    def _get_engine(self):
        if self._engine is None:
            if self.version == "1":
                # MySQL
                puerto = self.port if self.port else 3306
                conn_str = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            else:
                # PostgreSQL
                puerto = self.port if self.port else 5432
                conn_str = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            
            self._engine = create_engine(conn_str)
        return self._engine

    def close(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None
            logger.info("Conexiones de base de datos cerradas.")
