
import sys
import os
from extract import Extract 
from transform_precios_minoristas import Transform
from load import conexionBaseDatos
from pandas import to_datetime

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))


if __name__ == "__main__":

    #Extraccion de archivos
    Extract().descargar_archivo()

    #Obtencion de DATAFRAMES 
    instancia_transform = Transform()
    df_rem_precios_minoristas = instancia_transform.get_historico_precios_minoristas()
    df_rem_cambio_nominal = instancia_transform.get_historico_cambio_nominal()

    #Carga de datos
    instancia_load = conexionBaseDatos(host_dbb,user_dbb,pass_dbb,dbb_datalake)
    instancia_load.main(df_rem_precios_minoristas,df_rem_cambio_nominal)
