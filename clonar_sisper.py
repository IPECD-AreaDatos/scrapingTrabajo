import os
import logging
import pandas as pd
import pymysql
import psycopg2
from sqlalchemy import create_engine, text # Importamos text para SQL seguro
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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
                logger.info(f"[OK] Conexión MySQL establecida con Base Vieja v1")
            else:
                puerto = int(self.port) if self.port else 5432
                conn_str = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
                self.conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, database=self.database, port=puerto)
                logger.info(f"[OK] Conexión PostgreSQL establecida con Base Nueva v2")
            self.engine = create_engine(conn_str)
        except Exception as e:
            logger.error(f"Error al conectar a la base local: {e}")
            raise

    def cerrar(self):
        if self.conn: self.conn.close()
        if self.engine: self.engine.dispose()

def ejecutar_clonacion(tabla_origen, tabla_destino):
    load_dotenv()
    
    ORIGEN = {
        "host": "10.1.90.2",
        "user": "estadistica",
        "pass": "c3ns0$2026",
        "db": "sisper",
        "port": "5432"
    }

    tanda_size = 10000 
    offset = 0
    total_filas = 0
    primera_tanda = True
    nombre_csv = f"{tabla_destino}.csv"

    try:
        # 1. Conexión de ORIGEN con timeout largo
        engine_ext = create_engine(
            f"postgresql+psycopg2://{ORIGEN['user']}:{ORIGEN['pass']}@{ORIGEN['host']}:{ORIGEN['port']}/{ORIGEN['db']}",
            connect_args={'connect_timeout': 60}
        )
        
        # 2. SELECCIÓN DE DESTINO
        version = os.getenv('DB_VERSION', '1') # Por defecto v1
        host_dest = os.getenv('HOST_DBB1') if version == '1' else os.getenv('HOST_DBB2')
        user_dest = os.getenv('USER_DBB1') if version == '1' else os.getenv('USER_DBB2')
        pass_dest = os.getenv('PASSWORD_DBB1') if version == '1' else os.getenv('PASSWORD_DBB2')
        db_dest = 'conexiones_externas'

        loader = CargadorSelector(host_dest, user_dest, pass_dest, db_dest, version=version)
        loader.conectar()

        logger.info(f"=== INICIANDO CLONACIÓN PAGINADA (Tandas de {tanda_size}) ===")

        while True:
            # 3. LECTURA PAGINADA (LIMIT y OFFSET)
            # Esto obliga al servidor a darte solo un pedazo por vez
            query = text(f"SELECT * FROM {tabla_origen} LIMIT {tanda_size} OFFSET {offset}")
            
            logger.info(f"Descargando tanda desde fila {offset}...")
            df_chunk = pd.read_sql(query, engine_ext)

            # Si no vienen más filas, terminamos
            if df_chunk.empty:
                break

            # 4. CARGA A BASE DE DATOS
            modo = 'replace' if primera_tanda else 'append'
            df_chunk.to_sql(tabla_destino, loader.engine, schema='public', if_exists=modo, index=False)

            # 5. GENERACIÓN DE CSV INCREMENTAL
            modo_csv = 'w' if primera_tanda else 'a'
            df_chunk.to_csv(nombre_csv, index=False, encoding='utf-8-sig', sep=';', mode=modo_csv, header=primera_tanda)

            # Actualizar contadores
            total_filas += len(df_chunk)
            offset += tanda_size
            primera_tanda = False
            
            logger.info(f"[OK] Tanda procesada. Total acumulado: {total_filas} filas.")

        logger.info(f"=== PROCESO COMPLETADO: {total_filas} registros en total ===")

    except Exception as e:
        logger.error(f"Error crítico: {e}")
    finally:
        if 'loader' in locals():
            loader.cerrar()

if __name__ == "__main__":
    ejecutar_clonacion("deyc.personal", "sisper")