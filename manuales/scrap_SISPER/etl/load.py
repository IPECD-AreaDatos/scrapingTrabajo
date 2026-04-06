import os
import pymysql
import psycopg2
from sqlalchemy import create_engine
import logging

logger = logging.getLogger(__name__)

class CargadorSelector:
    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = str(version)
        self.conn = None
        self.engine = None

    def conectar(self):
        try:
            if self.version == "1":
                puerto = int(self.port) if self.port else 3306
                conn_str = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
                self.conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database, port=puerto)
                logger.info(f"[OK] Conexión MySQL establecida v1")
            else:
                puerto = int(self.port) if self.port else 5432
                conn_str = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
                self.conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, database=self.database, port=puerto)
                logger.info(f"[OK] Conexión PostgreSQL establecida v2")
            self.engine = create_engine(conn_str)
        except Exception as e:
            logger.error(f"Error al conectar a la base local: {e}")
            raise

    def cerrar(self):
        if self.conn: self.conn.close()
        if self.engine: self.engine.dispose()

def load_to_dest(df_chunk, loader, tabla_destino, primera_tanda=False, nombre_csv='sisper.csv'):
    """
    Carga de un dataframe a MySQL y lo añade incrementalmente al CSV.
    """
    # 1. Carga a MySQL Optimizado
    modo = 'replace' if primera_tanda else 'append'
    schema_dest = 'public' if loader.version == '2' else None
    
    logger.info(f"Subiendo {len(df_chunk)} registros a la base de datos ({modo})...")
    df_chunk.to_sql(
        tabla_destino, 
        loader.engine, 
        schema=schema_dest, 
        if_exists=modo, 
        index=False, 
        chunksize=2000, 
        method='multi' if loader.version == '1' else None # Solo MySQL necesita multi para velocidad
    )
    
    # 2. Guardado en CSV (Incremental)
    modo_csv = 'w' if primera_tanda else 'a'
    df_chunk.to_csv(nombre_csv, index=False, encoding='utf-8-sig', sep=';', mode=modo_csv, header=primera_tanda)
    logger.info(f"[OK] Carga incremental en {nombre_csv} completada.")
