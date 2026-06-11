"""
LOAD - Módulo de carga de datos Supermercados
Responsabilidad: Cargar datos si hay novedades (incremental por conteo de filas)
"""
import logging
import pandas as pd
import pymysql
import psycopg2
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class LoadSupermercados:
    """Carga los datos de supermercados a MySQL/PostgreSQL de forma híbrida."""

    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version)
        self.tabla = "supermercado"
        self.engine = None

    def _conectar(self):
        """Crea el motor de conexión dinámico según la versión."""
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

    def load(self, df: pd.DataFrame) -> bool:
        """Carga el DataFrame mediante una estrategia de reemplazo incremental."""
        self._conectar()
        schema = self._get_schema()
        full_table = f"{schema}.{self.tabla}" if schema else self.tabla

        
        
        # 1. Comparativa de conteo de filas
        with self.engine.connect() as conn:
            try:
                res = conn.execute(text(f"SELECT COUNT(*), MAX(fecha) FROM {full_table}")).fetchone()
                len_bdd = res[0]
                fecha_max_db = res[1]
            except Exception:
                len_bdd = 0
                fecha_max_db = None
                logger.info(f"Tabla '{self.tabla}' no existe, se creará al insertar.")

        fecha_max_extract = pd.to_datetime(df['fecha']).max()
        fecha_db_str = fecha_max_db.strftime('%Y-%m-%d') if hasattr(fecha_max_db, 'strftime') else 'Ninguna (Tabla vacía)'
        fecha_ext_str = fecha_max_extract.strftime('%Y-%m-%d') if hasattr(fecha_max_extract, 'strftime') else str(fecha_max_extract)

        logger.info(f"[LOAD] Comparación de fechas -> Base: {fecha_db_str} | Extraído: {fecha_ext_str}")
        
        # 2. Carga si hay novedades
        if len(df) > len_bdd:
            logger.info("[LOAD] ¡Datos nuevos detectados! Se reemplazarán los registros actuales.")
            with self.engine.begin() as conn:
                # Truncate seguro
                if self.version == "2":
                    conn.execute(text(f"TRUNCATE TABLE {full_table} CASCADE"))
                else:
                    conn.execute(text(f"TRUNCATE TABLE {full_table}"))
                
                # Inserción
                df.to_sql(
                    name=self.tabla, 
                    con=conn, 
                    schema=schema, 
                    if_exists='append', 
                    index=False, 
                    method='multi'
                )
            
            logger.info("[LOAD] Carga a la base completada.")
            return True
        
        logger.info(f"[LOAD] No hay datos nuevos. La base llega hasta {fecha_db_str} y se extrajo hasta {fecha_ext_str}. No se sube a la base ni al Sheets.")
        return False

    def close(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None