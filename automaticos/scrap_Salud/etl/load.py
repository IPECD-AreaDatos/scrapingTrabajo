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
        
        # 1. Obtener cuántas filas ya existen en la base
        try:
            with self.engine.connect() as conn:
                res = conn.execute(text(f"SELECT COUNT(*) FROM {self.tabla}"))
                len_bdd = res.scalar()
        except Exception:
            len_bdd = 0
            logger.info(f"Tabla '{self.tabla}' no encontrada. Se creará al insertar.")

        len_df = len(df)
        logger.info("[LOAD] BD: %d filas | DataFrame: %d filas", len_bdd, len_df)

        # 2. Si el Sheets tiene más filas que la BD, subimos la diferencia
        if len_df > len_bdd:
            df_nuevos = df.tail(len_df - len_bdd).copy()
            
            # Inserción en esquema public
            df_nuevos.to_sql(
                name=self.tabla, 
                con=self.engine, 
                schema='public', 
                if_exists='append', 
                index=False,
                method='multi'
            )
            logger.info("[LOAD] %d filas nuevas cargadas en salud.", len(df_nuevos))
            return True
        
        logger.info("[LOAD] No hay registros nuevos para procesar.")
        return False

    def close(self):
        """Cierra el engine de SQLAlchemy."""
        if self.engine:
            self.engine.dispose()
            self.engine = None