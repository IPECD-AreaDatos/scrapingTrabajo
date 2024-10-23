from readData import  homePage
import os
import sys 
from loadDataBase import load_database


# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_SOCIO'))

if __name__ == '__main__':
    df = homePage().construir_df_estimaciones(host_dbb,user_dbb,pass_dbb,dbb_datalake)
    print(df)
    instancia_bdd = load_database(host_dbb,user_dbb,pass_dbb,dbb_datalake)
    instancia_bdd.carga_datos(df)
