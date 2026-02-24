import os
import logging
import pandas as pd
import pymysql
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CargadorSelector:
    def __init__(self, host, user, password, database, port=None, version="2"):
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
            # Lógica para Base Vieja (v1) -> Motor MySQL
            if self.version == "1":
                puerto = int(self.port) if self.port else 3306
                conn_str = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
                self.conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database, port=puerto)
                logger.info(f"[OK] Conexión MySQL establecida con Base Vieja v1")
            else:
                # Lógica para Base Nueva (v2) -> Motor PostgreSQL
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
    
    # --- DATOS DE ORIGEN (SISPER - PostgreSQL) ---
    ORIGEN = {
        "host": "10.1.90.2",
        "user": "estadistica",
        "pass": "c3ns0$2026",
        "db": "sisper",
        "port": "5432"
    }

    try:
        # 1. EXTRACT: Leer desde la base externa (Postgres)
        logger.info(f"=== PASO 1: Descargando '{tabla_origen}' desde SISPER ===")
        print("pre")
        engine_ext = create_engine(f"postgresql+psycopg2://{ORIGEN['user']}:{ORIGEN['pass']}@{ORIGEN['host']}:{ORIGEN['port']}/{ORIGEN['db']}")
        print("post")
        df = pd.read_sql(f"SELECT * FROM {tabla_origen}", engine_ext)
        print("pre")
        logger.info(f"[OK] Se descargaron {len(df)} registros.")

        # 2. SELECCIÓN DE DESTINO (v1 MySQL por defecto)
        version = os.getenv('DB_VERSION', '1')
        host_dest = os.getenv('HOST_DBB1') if version == '1' else os.getenv('HOST_DBB2')
        user_dest = os.getenv('USER_DBB1') if version == '1' else os.getenv('USER_DBB2')
        pass_dest = os.getenv('PASSWORD_DBB1') if version == '1' else os.getenv('PASSWORD_DBB2')
        db_dest = 'conexiones_externas'

        # 3. LOAD: Crear tabla y cargar datos en tu MySQL
        loader = CargadorSelector(host_dest, user_dest, pass_dest, db_dest, version=version)
        loader.conectar()

        logger.info(f"=== PASO 2: Creando y cargando tabla '{tabla_destino}' en tu base local ===")
        # 'if_exists=replace' crea la tabla automáticamente si no existe en MySQL
        df.to_sql(tabla_destino, loader.engine, if_exists='replace', index=False)

        nombre_csv = f"{tabla_destino}.csv"
        df.to_csv(nombre_csv, index=False, encoding='utf-8-sig', sep=';')
        logger.info(f"=== PASO 3: CSV generado exitosamente como '{nombre_csv}' ===")
        
        logger.info(f"=== PROCESO COMPLETADO: La tabla '{tabla_destino}' ya está disponible en tu MySQL ===")

    except Exception as e:
        logger.error(f"Error crítico: {e}")
    finally:
        if 'loader' in locals():
            loader.cerrar()

if __name__ == "__main__":
    ejecutar_clonacion("deyc.personal", "sisper")