from readDataWebPage import readDataWebPage
from loadDataBase import loadDataBase
import os
import sys

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))

if __name__ == '__main__':
    df = readDataWebPage().extract_data()
    print(df)

    credenciales = loadDataBase(host_dbb,user_dbb,pass_dbb,dbb_datalake).conectar_bdd()

    credenciales.cargaBaseDatos(df)

