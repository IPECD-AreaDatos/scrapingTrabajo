import os
from dotenv import load_dotenv
from conect_bdd import ConexionBaseDatos
from extract import Extraccion
from transform import Transformacion
from save_data_sheet import ReadSheets

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Obtener las variables de entorno
host_dbb = os.getenv('HOST_DBB')
user_dbb = os.getenv('USER_DBB')
pass_dbb = os.getenv('PASSWORD_DBB')
dbb_datalake = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

# Verificar que todas las variables de entorno están cargadas
if not all([host_dbb, user_dbb, pass_dbb, dbb_datalake]):
    raise ValueError("Faltan variables de entorno necesarias para la conexión.")

if __name__ == "__main__":
    # Realizar la extracción de datos
    extraccion = Extraccion()
    extraccion.descargar_archivo()

    # Transformación y armado del DataFrame de combustible
    df_combustible = Transformacion().crear_df()

    # Conexión a la base de datos
    conexion_bdd = ConexionBaseDatos(host_dbb, user_dbb, pass_dbb, dbb_datalake)
    
    # Cargar datos en la base y obtener bandera de éxito
    bandera = conexion_bdd.main(df_combustible)
    
    # Imprimir el estado de la carga
    print(f"-- Condición de carga en la base de datos: {bandera}")
    
    # Si la bandera es True, actualizamos el Google Sheet
    if bandera:
        conexion_excel = ReadSheets(host_dbb, user_dbb, pass_dbb, dbb_datalake).conectar_bdd()
        conexion_excel.cargar_datos()
        print("Hoja de cálculo actualizada.")
    else:
        print(" ** NO SE CONSIDERA NECESARIO ACTUALIZAR GOOGLE SHEET ** ")
