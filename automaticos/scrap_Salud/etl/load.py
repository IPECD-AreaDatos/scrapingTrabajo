import logging
import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class Load:
    def __init__(self, host, user, password, database, port=5432):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.engine = None
        self.tabla = "derivaciones_red_obstetricia" # Nombre de la tabla en Postgres

    def conectar_bdd(self):
        """Establece la conexión a PostgreSQL."""
        if not self.engine:
            try:
                url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
                self.engine = create_engine(url)
                logger.info("[OK] Conectado a PostgreSQL - Base de Salud")
            except Exception as e:
                logger.error(f"[ERROR] Conexión fallida: {e}")
                raise
        return self

    def load(self, df: pd.DataFrame) -> bool:
        """Carga solo las filas nuevas de forma incremental."""
        self.conectar_bdd()
        
        try:
            logger.info("[LOAD] Iniciando reemplazo de tabla '%s' con %d filas.", self.tabla, len(df))
            
            # El parámetro 'replace' se encarga de: 
            # 1. DROP TABLE IF EXISTS
            # 2. CREATE TABLE
            # 3. INSERT
            df.to_sql(
                name=self.tabla, 
                con=self.engine, 
                schema='public', 
                if_exists='replace', 
                index=False,
                method='multi',
                chunksize=1000 # Opcional: mejora el rendimiento en cargas grandes
            )
            
            logger.info("[LOAD] Tabla '%s' reemplazada con éxito.", self.tabla)
            return True
            
        except Exception as e:
            logger.error(f"[ERROR] Falló la carga por reemplazo: {e}")
            return False

    def close(self):
        """Cierra el engine de SQLAlchemy."""
        if self.engine:
            self.engine.dispose()
            self.engine = None