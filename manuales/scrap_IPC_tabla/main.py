"""
Archivo destinado a construir la tabla correspondiente a Variaciones de distintos tipos de IPC.

IPC's que se tienen cuenta:

    - IPC a nivel NACIONAL
    - IPC NEA
    - IPC CABA
    - IPC ONLINE
    - IPC REM Precios minorista

    * Todos estos datos se extraen de "Datalake_economico"

Al ser una tabla analitica, la almacenaremos en dwh_economico

"""

from create_df import ExtractDataBDD
from loaddb import Load
import os
import sys

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))
dbb_dwh = (os.getenv('NAME_DBB_DWH_ECONOMICO'))


if __name__ == '__main__':
    
    #Creamos la intancia para buscar los datos
    instancia = ExtractDataBDD(host_dbb,user_dbb,pass_dbb,dbb_datalake)
    df = instancia.main()#--> Obtenemos los datos en un DF

    print(df)

    #Cargamos la BDD --> En este caso indicamos que la base de datos sera 'dwh_economico' | No creamos instancia, ya que solo cambiariamos esta cadena
    credenciales = Load(host_dbb,user_dbb,pass_dbb,dbb_dwh)
    credenciales.main(df)


