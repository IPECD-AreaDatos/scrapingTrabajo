"""
LOAD - Módulo de carga de datos IPICORR
Responsabilidad: Cargar solo filas nuevas a MySQL (incremental)
"""
import logging
import pandas as pd
import psycopg2
import pymysql
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

TABLA = "ipicorr"

class LoadIPICORR:
    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version)
        self.conn = None
        self.engine = None
        self.tabla = "ipicorr"

    def load(self, df: pd.DataFrame) -> bool:
        self.conectar_bdd()
        tabla_full = f"{self._get_schema_prefix()}{self.tabla}"
        
        # 1. Obtener conteo actual de forma segura
        with self.engine.connect() as conn:
            # Usamos text() para compatibilidad cross-engine
            try:
                res = conn.execute(text(f"SELECT COUNT(*) FROM {tabla_full}"))
                len_bdd = res.scalar()
            except Exception:
                len_bdd = 0
                logger.info(f"Tabla '{self.tabla}' no encontrada. Se creará al insertar.")

        len_df = len(df)
        logger.info("[LOAD] BD: %d filas | DataFrame: %d filas", len_bdd, len_df)

        if len_df > len_bdd:
            df_nuevos = df.tail(len_df - len_bdd).copy()
            df_nuevos['fecha'] = pd.to_datetime(df_nuevos['fecha']).dt.date
            
            # 2. Inserción
            schema_arg = "public" if self.version == "2" else None
            df_nuevos.to_sql(
                name=self.tabla, 
                con=self.engine, 
                schema=schema_arg, 
                if_exists='append', 
                index=False,
                method='multi'
            )
            logger.info("[LOAD] %d filas nuevas cargadas.", len(df_nuevos))
            return True
        
        logger.info("[LOAD] No hay datos nuevos para IPICORR.")
        return False

    def conectar_bdd(self):
        """Detecta el motor y establece la conexión."""
        if not self.conn:
            try:
                if self.version == "1":
                    # --- MySQL ---
                    puerto = int(self.port) if self.port else 3306
                    self.conn = pymysql.connect(
                        host=self.host, user=self.user, password=self.password, 
                        database=self.database, port=puerto
                    )
                    url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
                else:
                    # --- PostgreSQL ---
                    puerto = int(self.port) if self.port else 5432
                    self.conn = psycopg2.connect(
                        host=self.host, user=self.user, password=self.password, 
                        database=self.database, port=puerto
                    )
                    url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
                
                self.engine = create_engine(url)
                logger.info(f"[OK] Conectado a {'MySQL' if self.version=='1' else 'PostgreSQL'} (v{self.version})")
            except Exception as e:
                logger.error(f"[ERROR] Conexión fallida: {e}")
                raise
        return self

    def _get_schema_prefix(self):
        return "public." if self.version == "2" else ""

    def close(self):
        try:
            if self.conn: self.conn.close()
            if self.engine: self.engine.dispose()
            self.conn = self.engine = None
        except Exception as e:
            logger.warning(f"Error al cerrar conexión: {e}")