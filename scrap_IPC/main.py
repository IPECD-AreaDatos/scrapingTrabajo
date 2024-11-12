from transform_region import TransformRegiones
from carga_db import conexcionBaseDatos
import os
import sys
from homePage import HomePage
from correo2 import InformeIPC
from correo import Correo
from dotenv import load_dotenv
from datetime import datetime

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))


if __name__ == '__main__':

    #Descargar EXCEL - Tambien almacenamos las rutas que usaremos
    home_page = HomePage()    
    home_page.descargar_archivo()
    
    # Armado del df de ipc variaciones
    instancia_transform = TransformRegiones(host_dbb, user_dbb,pass_dbb, dbb_datalake)
    df = instancia_transform.main()

    print(df[df['fecha'] == '2024-08-01'])

    #Creamos instancia de BDD y realizamos verficacion de carga. Si hay carga, la bandera sera True, sino False
    instancia_bdd = conexcionBaseDatos(host_dbb, user_dbb,pass_dbb, dbb_datalake)
    bandera = instancia_bdd.main(df)


    if bandera:
        Correo(host_dbb, user_dbb,pass_dbb, dbb_datalake).main()
        print("Correo enviado!")
    else:
        print("Correo no enviado!")

    
