from extract import ExtractSheet
from transform import Transform
from load import Database
import os
import sys

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_dwh = (os.getenv('NAME_DBB_DWH_ECONOMICO'))


if __name__ == '__main__':

    #Extraccion y Transformacion de datos semoforo INTERANUAL
    df_semaforo_interanual = ExtractSheet().extract_sheet_internual()
    print("aca")
    print(df_semaforo_interanual)
    print("aca")
    df_interanual_transformado = Transform().transform_data(df_semaforo_interanual)

    #Extraccion y Transformacion de datos semoforo INTERMENSUAL
    df_semaforo_intermensual = ExtractSheet().extract_sheet_intermensual()
    print(df_semaforo_intermensual)
    print("aca")
    df_intermensual_transformado = Transform().transform_data(df_semaforo_intermensual)
    print(df_intermensual_transformado)

    #Almacenado en BDD
    instancia_bdd = Database(host_dbb,user_dbb,pass_dbb,dbb_dwh)
    instancia_bdd.load_data(df_interanual_transformado, df_intermensual_transformado)

    print("* Los indicadores de SEMAFORO han sido cargados")

    