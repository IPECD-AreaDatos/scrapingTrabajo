from homePage_CBT import HomePageCBT
from homePage_Pobreza import HomePagePobreza
from Transform_CbtCba import loadXLSDataCBT
from connectionDataBase import connection_db
import os
import sys 
import platform
# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)


#Tunel




from credenciales_bdd import Credenciales
from credenciales_tunel import CredencialesTunel

if __name__ == '__main__':

    credenciales = Credenciales()

    """    
    home_page_CBT = HomePageCBT()
    home_page_CBT.descargar_archivo()

    home_page_Pobreza= HomePagePobreza()
    home_page_Pobreza.descargar_archivo()


    loadXLSDataCBT().readData()

    instancia = connection_db(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    instancia.carga_db()

    print("- Finalizacion de revison de CBT")

    """

    #=== CREDENCIALES DE SSH Y BDD
    cred_tunel = CredencialesTunel()


    #=== SECCION DE DATALAKE

    df = loadXLSDataCBT().transform_datalake() #--> Transformar y concatenar datos del EXCEL
    
    print(df)
    exit()
    #Deteccion del sistema operativo - en base a esto la carga y la operabilidad varian
    system_operative = platform.system() #--> Retorna el nombre del sistema operativo (windows / linux)

    #Conectamos al tunel, y a la bdd
    conexion_datalake = connection_db(
        cred_tunel.ssh_host,
        cred_tunel.ssh_user,
        cred_tunel.ssh_pem_key_path,
        cred_tunel.mysql_host,
        cred_tunel.mysql_port,
        cred_tunel.mysql_user,
        cred_tunel.mysql_password,
        system_operative,
        'datalake_sociodemografico' #--> La base de datos se especifica
        )
    conexion_datalake.tunelizacion()
    conexion_datalake.load_datalake(df) #--> Cargamos la bdd


    #==== SECCCION DEL DATAWAREHOUSE

