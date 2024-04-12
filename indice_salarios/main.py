from extract import HomePage
from transform import Transformer
from load import Database
import os
import sys


#Ruta de SCRAPPING trabajo
script_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
credenciales_dir = os.path.join(script_dir, "Credenciales_folder") #--> Añadimos carpeta de credenciales folder

# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)

# Importar las credenciales
from credenciales_bdd import Credenciales

credenciales = Credenciales('datalake_economico')

if __name__ == '__main__':


    #Descarga del archivo
    HomePage().descargar_archivo()

    #Proceso de transformacion
    df = Transformer().transform_data_main()

    #Almacenamiento de datos
    Database(credenciales.host, credenciales.user, credenciales.password, credenciales.database).load_data(df)