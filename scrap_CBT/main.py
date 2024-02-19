from homePage_CBT import HomePageCBT
from homePage_Pobreza import HomePagePobreza
from Transform_CbtCba import loadXLSDataCBT
from connectionDataBase import connection_db
import os
import sys 

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales = Credenciales('ipecd_economico')


if __name__ == '__main__':
    #ZONA DE EXTRACT -- Donde se buscan los datos
    home_page_CBT = HomePageCBT()
    home_page_CBT.descargar_archivo()

    home_page_Pobreza= HomePagePobreza()
    home_page_Pobreza.descargar_archivo()


    #Creamos la instancia con la que logramos las operabilidad de la BDD
    instancia_conexion_bdd = connection_db(credenciales.host, credenciales.user, credenciales.password, credenciales.database)

    #=== SECCION DE DATALAKE
    df = loadXLSDataCBT().transform_datalake() #--> Transformar y concatenar datos del EXCEL
    instancia_conexion_bdd.connect_db()
    instancia_conexion_bdd.load_datalake(df) #--> Cargamos la bdd


    #==== SECCCION DEL DATAWAREHOUSE

    #instancia_conexion_bdd.table_a1()

