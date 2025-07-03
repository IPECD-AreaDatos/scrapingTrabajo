from homePage import HomePage
from loadCSVData import LoadCSVData
from loadCSVPuestosTotal import LoadCSVDataPuestosTotal
import os
import sys
from loadCSVDataDepartamentos import LoadCSVDataDepartamentos

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))


if __name__ == '__main__':
    #Carga de documento de departamentos
    #LoadCSVDataDepartamentos().loadInDataBase(host_dbb,user_dbb,pass_dbb,dbb_datalake)
    home_page = HomePage()
    home_page.descargar_archivo()
    instancia = LoadCSVData(host_dbb,user_dbb,pass_dbb,dbb_datalake)
    instancia_privado = LoadCSVDataPuestosTotal(host_dbb,user_dbb,pass_dbb,dbb_datalake)
    instancia.loadInDataBase()
    instancia_privado.loadInDataBase()