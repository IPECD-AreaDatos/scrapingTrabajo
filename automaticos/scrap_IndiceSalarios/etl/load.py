"""
LOAD - Módulo de carga de datos Índice de Salarios
Responsabilidad: Cargar solo filas nuevas a MySQL (append incremental)
"""
import logging
import pandas as pd
import psycopg2
import pymysql
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class LoadIndiceSalarios:
    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version)
        self.engine = None
        self.tabla = "indice_salario"

    def _conectar(self):
        """Crea el motor de conexión según versión (MySQL o PostgreSQL)."""
        if self.engine is None:
            if self.version == "1":
                puerto = int(self.port) if self.port else 3306
                url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            else:
                puerto = int(self.port) if self.port else 5432
                url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            
            self.engine = create_engine(url, echo=False)
            logger.info(f"[OK] Motor conectado a '{self.database}' (v{self.version})")

    def load(self, df: pd.DataFrame) -> bool:
        self._conectar()
        schema = self._get_schema()
        full_table = f"{schema}.{self.tabla}" if schema else self.tabla
        
        # 1. Asegurar formato de fecha
        df['fecha'] = pd.to_datetime(df['fecha']).dt.date
        
        # 2. Lógica incremental: Borrar fechas existentes para evitar duplicados
        with self.engine.begin() as conn:
            # Obtener fechas del DF que ya están en la BD para no re-insertarlas
            fechas = tuple(df['fecha'].unique().tolist())
            if fechas:
                conn.execute(text(f"DELETE FROM {full_table} WHERE fecha IN :fechas"), {"fechas": fechas})
            
            # 3. Insertar nuevos datos
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

    def _get_schema(self):
        return "public" if self.version == "2" else None

    def close(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None