"""
LOAD - Módulo de carga de datos SalarioSPTotal
Responsabilidad: Cargar los 2 DataFrames a MySQL (incremental)
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class LoadSalarioSPTotal:
    """Carga los DataFrames de salarios de forma incremental (Híbrido MySQL/Postgres)."""

    TABLA_SP    = "salarios_sector_privado"
    TABLA_TOTAL = "salarios_total"

    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version)
        self.engine = None

    def _conectar(self):
        """Crea el motor de conexión dinámico."""
        if self.engine is None:
            if self.version == "1":  # MySQL
                puerto = int(self.port) if self.port else 3306
                url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            else:  # PostgreSQL
                puerto = int(self.port) if self.port else 5432
                url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            
            self.engine = create_engine(url, echo=False)
            logger.info(f"[OK] Motor conectado a '{self.database}' (v{self.version})")

    def _get_schema(self):
        return "public" if self.version == "2" else None

    def load(self, df_sp: pd.DataFrame, df_total: pd.DataFrame):
        self._conectar()
        schema = self._get_schema()
        
        self._cargar(df_sp, self.TABLA_SP, schema)
        self._cargar(df_total, self.TABLA_TOTAL, schema)

    def _cargar(self, df: pd.DataFrame, tabla: str, schema: str):
        full_table = f"{schema}.{tabla}" if schema else tabla
        
        # 1. Asegurar formato de fecha
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date
        
        # 2. Inserción incremental mediante borrado de fechas duplicadas
        # Esto evita el problema de contar filas y es más robusto ante re-procesos
        fechas_a_actualizar = tuple(df['fecha'].unique().tolist())
        
        with self.engine.begin() as conn:
            # Borramos registros de las fechas que estamos recibiendo para evitar duplicados
            if fechas_a_actualizar:
                conn.execute(text(f"DELETE FROM {full_table} WHERE fecha IN :fechas"), 
                             {"fechas": fechas_a_actualizar})
            
            # Cargamos los nuevos datos
            df.to_sql(
                name=tabla, 
                con=conn, 
                schema=schema, 
                if_exists='append', 
                index=False, 
                method='multi'
            )
            
        logger.info("[LOAD] '%s' actualizada correctamente.", tabla)

    def close(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("[LOAD] Conexión a BD cerrada.")