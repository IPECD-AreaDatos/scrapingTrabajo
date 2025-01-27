import os
import sys
import logging
from dotenv import load_dotenv
from extract import HomePage
from transform import Transformer
from load import Load

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Validar variables de entorno
required_env_vars = ['HOST_DBB', 'USER_DBB', 'PASSWORD_DBB', 'NAME_DBB_DATALAKE_ECONOMICO']
for var in required_env_vars:
    if not os.getenv(var):
        logging.error(f"La variable de entorno {var} no está definida.")
        sys.exit(1)

host_dbb = os.getenv('HOST_DBB')
user_dbb = os.getenv('USER_DBB')
pass_dbb = os.getenv('PASSWORD_DBB')
dbb_datalake = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

def main():
    try:
        # Obtención del archivo
        logging.info("Descargando archivos...")
        HomePage().descargar_archivos()

        # Creación de instancia del transformador y generación de DataFrames
        logging.info("Transformando datos...")
        instancia_transformador = Transformer()
        df_emae_valores = instancia_transformador.construir_df_emae_valores()
        df_emae_variaciones = instancia_transformador.construir_df_emae_variaciones()

        # Carga de los DataFrames
        logging.info("Cargando datos en la base de datos...")
        instancia_load = Load(host=host_dbb, user=user_dbb, password=pass_dbb, database=dbb_datalake)
        instancia_load.main_load(df_emae_valores, df_emae_variaciones)

        logging.info("Proceso completado exitosamente.")

    except Exception as e:
        logging.error(f"Error en el proceso principal: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()