import os
import sys
from conexionBaseDatos import conexcionBaseDatos
from readSheetsCensoIPECD import readSheetsCensoIPECD

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_dwh = (os.getenv('NAME_DBB_DWH_SOCIO'))

if __name__ == "__main__":
    df_censo = readSheetsCensoIPECD().leer_datos_censo()
    conexion = conexcionBaseDatos(host_dbb,user_dbb,pass_dbb,dbb_dwh).conectar_bdd()
    conexion.cargaBaseDatos(df_censo)