from homePage_CBT import HomePageCBT
from homePage_Pobreza import HomePagePobreza
from loadXLSDataCBT import loadXLSDataCBT
from connectionDataBase import connection_db
import os
import sys 
# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)

from credenciales_bdd import Credenciales

if __name__ == '__main__':
    credenciales = Credenciales()
    home_page_CBT = HomePageCBT()
    home_page_CBT.descargar_archivo()
    
    home_page_Pobreza= HomePagePobreza()
    home_page_Pobreza.descargar_archivo()
    

    loadXLSDataCBT().readData()

    instancia = connection_db(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    instancia.carga_db()

    print("- Finalizacion de revison de CBT")



    #PRUEBAS DE DATALAKE

    df = loadXLSDataCBT.transform_datalake() #--> Transformar y concatenar datos del EXCEL
    connection_db(credenciales.host, credenciales.user, credenciales.password, "datalake_sociodemografico").load_datalake()

