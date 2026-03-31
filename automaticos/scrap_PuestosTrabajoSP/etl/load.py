"""
LOAD - Módulo de carga de datos PuestosTrabajoSP
Responsabilidad: Cargar las 2 tablas de puestos de trabajo (incremental) soportando v1 (MySQL) y v2 (PostgreSQL)
"""
import logging
import pandas as pd
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

TABLAS = {
    'privado': 'dp_puestostrabajo_sector_privado',
    'total':   'dp_puestostrabajo_total',
}

class LoadPuestosTrabajoSP:
    """Carga los 2 DataFrames de puestos de trabajo de forma incremental."""

    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version)
        self._engine = None

    def load(self, df_privado: pd.DataFrame, df_total: pd.DataFrame) -> bool:
        """
        Carga solo las filas nuevas en cada tabla.
        Returns:
            bool: True si se cargaron datos nuevos en alguna tabla
        """
        cargado = False
        cargado |= self._cargar_incremental(df_privado, TABLAS['privado'])
        cargado |= self._cargar_incremental(df_total,   TABLAS['total'])
        return cargado

    def _cargar_incremental(self, df: pd.DataFrame, tabla: str) -> bool:
        # Definir esquema según versión
        schema = "public" if self.version == "2" else None
        nombre_tabla_completo = f"{schema}.{tabla}" if schema else tabla

        try:
            # Obtener cantidad de filas actuales en la base de datos
            query = text(f"SELECT COUNT(*) FROM {nombre_tabla_completo}")
            with self._get_engine().connect() as conn:
                result = conn.execute(query)
                len_bdd = result.scalar() or 0

            len_df = len(df)
            logger.info("[LOAD] v%s - '%s' — BD: %d | DF: %d", self.version, tabla, len_bdd, len_df)

            if len_df > len_bdd:
                # Extraemos solo las filas que no están en la BDD
                df_nuevos = df.iloc[len_bdd:] 
                
                df_nuevos.to_sql(
                    name=tabla, 
                    con=self._get_engine(), 
                    schema=schema,
                    if_exists='append', 
                    index=False,
                    method='multi' if self.version == "2" else None
                )
                logger.info("[LOAD] v%s - %d filas nuevas cargadas en '%s'.", self.version, len(df_nuevos), tabla)
                return True
            
            logger.info("[LOAD] v%s - Sin datos nuevos para '%s'.", self.version, tabla)
            return False

        except Exception as e:
            logger.error("[LOAD ERROR] v%s en tabla %s: %s", self.version, tabla, e)
            raise

    def _get_engine(self):
        if self._engine is None:
            if self.version == "1":
                # Configuración para MySQL
                puerto = self.port if self.port else 3306
                conn_str = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            else:
                # Configuración para PostgreSQL (v2)
                puerto = self.port if self.port else 5432
                conn_str = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
            
            self._engine = create_engine(conn_str)
        return self._engine

    def close(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None
            logger.info("Conexiones de base de datos cerradas.")