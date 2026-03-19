import pandas as pd
from sqlalchemy import create_engine, text
import re
import logging

logger = logging.getLogger(__name__)

class connection_db:

    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version) # "1" para MySQL, "2" para Postgres
        self.tabla = "cbt_cba"
        self.engine = None

    #Conexion a la BDD
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

    #Objetivo: Almacenar los datos de CBA y CBT sin procesar en el datalake. Datos sin procesar
    def load_datalake(self,df: pd.DataFrame) -> bool:
        """Carga incremental híbrida para el Datalake."""
        self._conectar()
        schema = self._get_schema()
        full_table = f"{schema}.{self.tabla}" if schema else self.tabla

        # Aseguramos que los tipos de datos sean numéricos y redondeamos
        cols_numericas = ['cba_adulto', 'cbt_adulto', 'cba_hogar', 'cbt_hogar', 'cba_nea', 'cbt_nea']
        for col in cols_numericas:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

        # 2. Comparativa de conteo
        with self.engine.connect() as conn:
            try:
                res = conn.execute(text(f"SELECT COUNT(*) FROM {full_table}"))
                len_bdd = res.scalar()
            except Exception:
                len_bdd = 0
                logger.info(f"Tabla '{self.tabla}' no existe, se creará.")

        # 3. Carga si hay novedades
        if len(df) > len_bdd:
            # Ordenamos por fecha antes de insertar
            df = df.sort_values(by='fecha', ascending=True)
            
            with self.engine.begin() as conn:
                # Truncate
                if self.version == "2":
                    conn.execute(text(f"TRUNCATE TABLE {full_table} CASCADE"))
                else:
                    conn.execute(text(f"TRUNCATE TABLE {full_table}"))
                
                # Inserción masiva con SQLAlchemy (adiós a los loops de cursores)
                df.to_sql(
                    name=self.tabla, 
                    con=conn, 
                    schema=schema, 
                    if_exists='append', 
                    index=False, 
                    method='multi'
                )
            
            ultima_fecha = df['fecha'].iloc[-1]
            logger.info(f"[LOAD] '{self.tabla}' actualizada: {len(df)} filas. Último dato: {ultima_fecha}")
            return True
        
        logger.info(f"[LOAD] No hay datos nuevos. BDD: {len_bdd} filas / DF: {len(df)} filas.")
        return False
    
    def close(self):
        """Cierra el pool de conexiones del motor."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("Conexiones de base de datos cerradas.")