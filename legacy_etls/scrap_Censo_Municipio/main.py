import os
import sys
from readSheetsCensoMunicipio import readSheetsCensoMunicipio
from load_bdd import loadDataBase
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DWH_SOCIO'))


if __name__ == "__main__":
    df_censo = readSheetsCensoMunicipio().leer_datos_censo()
    conexion = loadDataBase(host_dbb, user_dbb, pass_dbb, dbb_datalake).conectar_bdd()
    conexion.carga_bdd(df_censo)