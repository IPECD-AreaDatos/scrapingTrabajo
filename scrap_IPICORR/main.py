from readSheets import readSheets
from connect_db import connect_db
from homePage_IPI import HomePage_IPI
from database_ipi import Database_ipi
import os
import sys
#Datos de la base de datos
host = '172.17.22.23'
user = 'team-datos'
password = 'HCj_BmbCtTuCv5}'
database = 'ipecd_economico'



# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)

from credenciales_bdd import Credenciales

credenciales = Credenciales()

# Crea una instancia de la clase "Credenciales"
instancia_credenciales = Credenciales()



if __name__ == "__main__":
    df = readSheets().tratar_datos()
    connect_db().connect(df, credenciales.host, credenciales.user, credenciales.password, credenciales.database)

 
    home_page = HomePage_IPI()
    home_page.descargar_archivo()
    df_ipi = home_page.construir_df()

    print(df_ipi)


    
    Database_ipi().cargar_datos(credenciales.host, credenciales.user, credenciales.password, credenciales.database,df_ipi)

