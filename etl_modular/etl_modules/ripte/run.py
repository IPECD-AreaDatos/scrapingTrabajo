import os
from dotenv import load_dotenv
from etl_modular.utils.logger import setup_logger
from .extract import extract_ripte_data, extract_latest_ripte_value
from .transform import transform_ripte_data
from .load import load_ripte_data, load_latest_ripte_value
from etl_modular.utils.db import ConexionBaseDatos

logger = setup_logger("ripte")

def run_ripte(mode='last'):

    load_dotenv()

    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    conexion_db = None

    try:
        conexion_db = ConexionBaseDatos(host, user, password, database)
        conexion_db.connect_db()

        if mode == 'historical':
            logger.info("📥 Iniciando carga *histórica* de RIPTE...")
            ruta = extract_ripte_data()
            df = transform_ripte_data(ruta)

            if df is not None and not df.empty:
                datos_cargados = load_ripte_data(df, host, user, password, database)
                logger.info("✅ Carga histórica de RIPTE finalizada.")
            else:
                logger.warning("⚠️ Archivo vacío o falló la transformación. Se omite la carga.")

        elif mode == 'last':
            logger.info("📥 Iniciando carga del *último valor* de RIPTE...")
            ultimo_valor = extract_latest_ripte_value()
            load_latest_ripte_value(ultimo_valor, host, user, password, database)
            logger.info("✅ Proceso de carga del último dato de RIPTE finalizado.")

        else:
            logger.warning(f"⚠️ Modo '{mode}' no reconocido. Use 'last' o 'historical'.")

    except Exception as e:
        logger.error(f"❌ Error en el proceso ETL de RIPTE: {e}")

    finally:
        if conexion_db:
            conexion_db.close_connections()
            logger.info("🔌 Conexión a base cerrada.")
            
            
            