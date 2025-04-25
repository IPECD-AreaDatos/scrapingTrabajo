import os
import sys
from readGoogleSheets import readGoogleSheets
from connect_db import DatabaseManager

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))


if _name_ == "_main_":
    #-----IPICORR--------
    df_ipicorr = readGoogleSheets().tratar_datos()
    DatabaseManager(host_dbb,user_dbb,pass_dbb,dbb_datalake).update_database_with_new_data(df_ipicorr)