from etl_modular.utils.db import ConexionBaseDatos
import logging
logger = logging.getLogger("emae") # Obtiene el logger ya configurado por run_emae

def load_emae_valores(df, db: ConexionBaseDatos):
    logger.info(" Cargando datos EMAE VALORES...")

    try:
        success = db.load_if_newer(df, table_name="emae", date_column="fecha")
        return success
    except Exception as e:
        logger.info(f" Error al cargar EMAE VALORES: {e}")
        return False


def load_emae_variaciones(df, db: ConexionBaseDatos):
    logger.info(" Cargando datos EMAE VARIACIONES...")

    try:
        db.cursor.execute("SELECT COUNT(*) FROM emae_variaciones")
        count_bdd = db.cursor.fetchone()[0]

        if len(df) > count_bdd:
            nuevos = df.tail(len(df) - count_bdd)
            db.insert_append("emae_variaciones", nuevos)
            logger.info(f" {len(nuevos)} registros nuevos insertados en emae_variaciones.")
            return True
        else:
            logger.info(" No hay variaciones nuevas para insertar.")
            return False
    except Exception as e:
        logger.error(f"Error al cargar EMAE VALORES: {e}")
        return False
