from readSheets import readSheets
from conectDataBase import conectDataBase

import os
host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))

if __name__ == '__main__':
    # Carga de diccionario de datos Clae
    df_diccionario = readSheets().readDataDiccionario()
    print(df_diccionario)

    # Conexión y carga a la base de datos
    db_connection = conectDataBase(host_dbb, user_dbb, pass_dbb, dbb_datalake)
    if db_connection.connect_db():
        db_connection.load_data_diccionario(df_diccionario)
        db_connection.cerrar_conexion()