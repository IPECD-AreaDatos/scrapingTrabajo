from extract import HomePage
from transform import Transform
from load import Load
import os
import sys

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))

if __name__ == "__main__":

    #Descarga del archivo
    HomePage().descargar_archivo()

    #Transformamos los datos
    df = Transform().extract_data_sheet()
    
    
    #Cargamos en el datalake
    Load(host_dbb,user_dbb,pass_dbb,dbb_datalake).load_datalake(df)
