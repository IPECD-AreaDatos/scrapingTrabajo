from extract import Extraccion
from dicc import Diccionario
from transform import Transformacion
from dotenv import load_dotenv
from datetime import datetime
from load import conexcionBaseDatos
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))

if __name__ == '__main__':    
    Extraccion().descargar_archivo()
    instancia = Diccionario(host_dbb, user_dbb,pass_dbb, dbb_datalake)
    instancia.main()
    instancia = Transformacion(host_dbb, user_dbb,pass_dbb, dbb_datalake)
    df = instancia.main()
    conexcion = conexcionBaseDatos(host_dbb, user_dbb,pass_dbb, dbb_datalake).main(df)
