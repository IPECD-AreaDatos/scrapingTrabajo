import sys
import os
import pandas as pd
from transform import Transform
from load import ConexionBase
# from dicc import Diccionario  <-- COMENTA O BORRA ESTA IMPORTACIÓN

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()
host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))

if __name__ == "__main__":

    # --- BORRAMOS LA CREACIÓN DE DICCIONARIOS ---
    # Ya no llamamos a Diccionario().construir_dicc() porque el Excel no existe
    # y los datos ya están en la base de datos.
    
    print("Iniciando proceso de transformación de CSVs...")

    # creacion del df final (Esto sigue igual)
    df = Transform().procesar_archivos()
    
    # Validaciones (Esto sigue igual)
    if not df.empty:
        print("df final creado con éxito.")
        # print(df.dtypes)
        
        df_negativos = df[(df["cant_personas_trabaj_up"] < 0) | (df["remuneracion"]  < 0) ]
        if not df_negativos.empty:
            print(" ATENCIÓN: Se encontraron valores negativos.")
            print(df_negativos)
        
        # Carga de datos
        print("Iniciando carga a Base de Datos...")
        instancia_load = ConexionBase(host_dbb, user_dbb, pass_dbb, dbb_datalake)
        
        # --- AQUÍ EL CAMBIO IMPORTANTE ---
        # Solo le pasamos el 'df' principal. Quitamos los diccionarios.
        instancia_load.main(df)
        
        print("¡Proceso finalizado con éxito!")
    else:
        print("No se generaron datos para cargar.")