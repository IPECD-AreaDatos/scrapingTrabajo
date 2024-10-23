import os
import sys
from readSheets import readSheets
from connect_db import connect_db


# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_dwh = (os.getenv('NAME_DBB_DWH_SOCIO'))


if __name__ ==  "__main__":

    df = readSheets().leer_datos_tasas()
    connect_db().connect(df, host_dbb,user_dbb,pass_dbb,dbb_dwh)