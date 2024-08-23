"""
En esta ocacion fue necesario durante el proceso de extraccion de los datos
una transformacion, esto porque el tratado de la tabla de DNRPA requeria de 
varias operaciones para concatenar las tablas de diferentes años entre si.
"""

from exctract import ExtractDnrpa
from exctract_last_year import ExtractLast
from load import conexionBaseDatos
from load_last import conexionBaseDatosLast

import os

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))


if __name__=='__main__':

    #Extraccion y TRANSFORMACION de datos
    df = ExtractLast().extraer_tablas()

    print(df)
    
    #Cargamos al datalake economico
    instancia_bdd = conexionBaseDatosLast(host_dbb,user_dbb,pass_dbb,dbb_datalake)
    instancia_bdd.cargar_datalake(df)

