import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from conect_bdd import ConexionBaseDatos
from extract import Extraccion
from transform import Transformacion
from save_data_sheet import ReadSheets

# Configuración básica de logging
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, 'ventas_combustible_scraper.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def cargar_variables_entorno():
    """Carga las variables de entorno necesarias para la conexión."""
    load_dotenv()

    host_dbb = os.getenv('HOST_DBB')
    user_dbb = os.getenv('USER_DBB')
    pass_dbb = os.getenv('PASSWORD_DBB')
    dbb_datalake = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    if not all([host_dbb, user_dbb, pass_dbb, dbb_datalake]):
        raise ValueError("Faltan variables de entorno necesarias para la conexión.")

    logger.info("Variables de entorno cargadas correctamente.")
    return host_dbb, user_dbb, pass_dbb, dbb_datalake


def realizar_extraccion_datos():
    """Realiza la extracción de datos desde la fuente."""
    logger.info("[EXTRACT] Iniciando la descarga de datos...")
    extraccion = Extraccion()
    extraccion.descargar_archivo()
    logger.info("[EXTRACT] Datos descargados exitosamente.")


def transformar_datos(transformador=None):
    """Realiza la transformación de los datos y genera el DataFrame de combustible."""
    logger.info("[TRANSFORM] Iniciando transformación...")
    transformador = transformador or Transformacion()
    df_combustible = transformador.crear_df()
    logger.info("[TRANSFORM] Transformación completa. Filas: %d", len(df_combustible))
    return df_combustible, transformador


def calcular_suma_por_fecha(transformador, df=None):
    """Calcula la suma de combustible para la última fecha disponible."""
    logger.info("[TRANSFORM] Calculando suma mensual para la última fecha...")
    suma_mensual = transformador.suma_por_fecha(df)
    logger.info("[TRANSFORM] Suma calculada: %s", suma_mensual)
    return suma_mensual


def cargar_datos_base_datos(df_combustible, host_dbb, user_dbb, pass_dbb, dbb_datalake):
    """Carga los datos transformados a la base de datos."""
    logger.info("[LOAD] Preparando conexión a la base de datos...")
    conexion_bdd = ConexionBaseDatos(host_dbb, user_dbb, pass_dbb, dbb_datalake)

    logger.info("[LOAD] Iniciando carga en base de datos...")
    carga_exitosa = conexion_bdd.main(df_combustible)
    estado = "éxito" if carga_exitosa else "fracaso"
    logger.info("[LOAD] Resultado de carga en BD: %s", estado)

    return carga_exitosa


def actualizar_hoja_google(carga_exitosa, suma_mensual, host_dbb, user_dbb, pass_dbb, dbb_datalake):
    """Actualiza la hoja de cálculo de Google si la carga a la base de datos fue exitosa."""
    if not carga_exitosa:
        logger.info("[LOAD] No se actualiza Google Sheets porque no hubo carga nueva.")
        return

    logger.info("[LOAD] Actualizando Google Sheets...")
    conexion_excel = ReadSheets(host_dbb, user_dbb, pass_dbb, dbb_datalake).conectar_bdd()
    conexion_excel.cargar_datos(suma_mensual)
    logger.info("[LOAD] Google Sheets actualizado correctamente.")


def main():
    inicio = datetime.now()
    logger.info("=" * 80)
    logger.info("=== INICIO ETL Ventas Combustible ===")
    logger.info("=" * 80)

    try:
        host_dbb, user_dbb, pass_dbb, dbb_datalake = cargar_variables_entorno()
        realizar_extraccion_datos()
        df_combustible, transformador = transformar_datos()
        suma_mensual = calcular_suma_por_fecha(transformador, df_combustible)
        carga_exitosa = cargar_datos_base_datos(df_combustible, host_dbb, user_dbb, pass_dbb, dbb_datalake)
        actualizar_hoja_google(carga_exitosa, suma_mensual, host_dbb, user_dbb, pass_dbb, dbb_datalake)

    except Exception as e:
        logger.error("Error en el proceso ETL de Ventas Combustible: %s", e, exc_info=True)
        raise
    finally:
        fin = datetime.now()
        duracion = (fin - inicio).total_seconds()
        logger.info("=== FIN ETL Ventas Combustible (%.2f segundos) ===", duracion)
        logger.info("=" * 80)


if __name__ == '__main__':
    main()
