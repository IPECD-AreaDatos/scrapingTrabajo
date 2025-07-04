import sys
import os
import pandas as pd
from transform import Transform
from load import ConexionBase
from dicc import Diccionario

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()
host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))

if __name__ == "__main__":

    dicc_seccion, dicc_grupo, dicc_ciiu = Diccionario().construir_dicc()

    # creacion del df final
    df = Transform().procesar_archivos()
    
    print("df final:")
    print(df)

    print(df.dtypes)

    df_negativos = df[(df["cant_personas_trabaj_up"] < 0) | (df["remuneracion"]  < 0) ]
    print(df_negativos)
    
    
    #Carga de datos
    instancia_load = ConexionBase(host_dbb, user_dbb, pass_dbb, dbb_datalake)
    instancia_load.main(df, dicc_seccion, dicc_grupo, dicc_ciiu)


