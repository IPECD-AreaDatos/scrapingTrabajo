from extract import HomePage
from transform import Transform
from load import Load
import os
import sys

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
from credenciales_bdd import Credenciales
credenciales = Credenciales("datalake_economico")

if __name__ == "__main__":

    #Descarga del archivo
    HomePage().descargar_archivo()

    #Transformamos los datos
    df = Transform().extract_data_sheet()
    print(df)
    print("\n")
    
    #Cargamos en el datalake
    Load(credenciales.host, credenciales.user, credenciales.password, credenciales.database).load_datalake(df)
