"""
LOAD - Módulo de carga de datos REM
Responsabilidad: Cargar precios minoristas y cambio nominal a MySQL
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class LoadREM:
    """Carga los datos del REM a MySQL/PostgreSQL de forma híbrida."""

    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version)
        self.engine = None
        self.tabla_cambio = "rem_cambio_nominal"

    def _conectar(self):
        """Crea el motor de conexión dinámico."""
        if self.engine is None:
            if self.version == "1":
                puerto = int(self.port) if self.port else 3306
                url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            else:
                puerto = int(self.port) if self.port else 5432
                url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            
            self.engine = create_engine(url, echo=False)
            logger.info(f"[OK] Motor conectado a '{self.database}' (v{self.version})")

    def _get_schema(self):
        return "public" if self.version == "2" else None

    def load(self, df_cambio: pd.DataFrame):
        """Carga la tabla de cambio nominal con Truncate + Replace."""
        self._conectar()
        schema = self._get_schema()
        full_table = f"{schema}.{self.tabla_cambio}" if schema else self.tabla_cambio

        with self.engine.begin() as conn:
            # Truncate seguro según motor
            if self.version == "2":
                conn.execute(text(f"TRUNCATE TABLE {full_table} CASCADE"))
            else:
                conn.execute(text(f"TRUNCATE TABLE {full_table}"))
            
            # Carga de datos
            df_cambio.to_sql(
                name=self.tabla_cambio, 
                con=conn, 
                schema=schema, 
                if_exists='append', 
                index=False, 
                method='multi'
            )
            
        logger.info("[LOAD] '%s' actualizada correctamente con %d filas.", self.tabla_cambio, len(df_cambio))

    def close(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None