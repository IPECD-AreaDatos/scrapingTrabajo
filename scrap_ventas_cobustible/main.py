import os
import sys
from conect_bdd import conexcionBaseDatos
from extract import Extraccion
from transform import Transformacion
from save_data_sheet import readSheets
from dotenv import load_dotenv
from save_data_sheet import readSheets

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))

if __name__ == "__main__":

    extraer = Extraccion()
    extraer.descargar_archivo()
        
    # Armado del df de combustible
    df_combustible = Transformacion().crear_df()

    # Carga de los dfs a la base
    instancia_bdd = conexcionBaseDatos(host_dbb, user_dbb,pass_dbb, dbb_datalake)

    # Banderas si se actualizaron las bases
    bandera = instancia_bdd.main(df_combustible)

    #Valor de bandera
    print(f"-- Condicion de carga en la base de datos: {bandera}")

    if bandera:
        conexion_excel = readSheets(host_dbb, user_dbb,pass_dbb, dbb_datalake).conectar_bdd()
        conexion_excel.cargar_datos()
        print("Sheet actualizado.")
    else:
        print(" ** NO SE CONSIDERA NECESARIO ACTUALIZAR GOOGLE SHEET ** ")



    