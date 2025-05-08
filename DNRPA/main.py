"""
En esta ocacion fue necesario durante el proceso de extraccion de los datos
una transformacion, esto porque el tratado de la tabla de DNRPA requeria de 
varias operaciones para concatenar las tablas de diferentes años entre si.
"""
import os

#En caso de que se requiera cargar toda la informacion de nuevo
from exctract_historico import ExtractHistoricalDnrpa
from load_historico import conexionBaseDatos

#En caso de que se requiera cargar solo la ultima informacion
from exctract_last_year import ExtractLastData
from load_last import conexionBaseDatosLast


# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))


if __name__=='__main__':
    #df_tota = ExtractHistoricalDnrpa().extraer_tablas()
    #conexionBaseDatos(host_dbb,user_dbb,pass_dbb,dbb_datalake).cargar_datalake(df_tota)
    #exit()
    
    #Extraccion y TRANSFORMACION de datos
    df = ExtractLastData().extraer_tablas()
    #Cargamos al datalake economico
    instancia_bdd = conexionBaseDatosLast(host_dbb,user_dbb,pass_dbb,dbb_datalake)
    instancia_bdd.cargar_datalake(df)

