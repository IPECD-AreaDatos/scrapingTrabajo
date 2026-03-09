"""
LOAD - Módulo de carga de datos SalarioMVM
Responsabilidad: Cargar solo filas nuevas a MySQL (incremental)
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class LoadSalarioMVM:
    """Carga los datos del Salario MVM a MySQL/PostgreSQL de forma incremental."""

    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version)
        self.tabla = 'salario_mvm'
        self.engine = None
        self.columnas = ['fecha', 'salario_mvm_mensual',
                         'salario_mvm_diario', 'salario_mvm_hora']

    def _conectar(self):
        """Crea el motor de conexión según la versión (v1: MySQL, v2: Postgres)."""
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
        self._conectar()
        schema = self._get_schema()
        full_table = f"{schema}.{self.tabla}" if schema else self.tabla
        
        # 1. Asegurar formato fecha (asumiendo que 'indice_tiempo' es la columna de fecha)
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date
        
        # 2. Inserción incremental mediante borrado de fechas duplicadas
        fechas_a_actualizar = tuple(df['fecha'].unique().tolist())
        
        with self.engine.begin() as conn:
            # Borrar registros existentes para las fechas que recibimos
            if fechas_a_actualizar:
                conn.execute(text(f"DELETE FROM {full_table} WHERE fecha IN :fechas"), 
                             {"fechas": fechas_a_actualizar})
            
            # 3. Cargar datos nuevos
            df[self.columnas].to_sql(
                name=self.tabla, 
                con=conn, 
                schema=schema, 
                if_exists='append', 
                index=False, 
                method='multi'
            )
            
        logger.info("[LOAD] '%s' actualizada: %d filas.", self.tabla, len(df))
        return True

    def close(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None