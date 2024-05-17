from extract import ExtractSheet
from transform import Transform
from load import Database
import os
import sys

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)


from credenciales_bdd import Credenciales


credenciales = Credenciales("dwh_economico")


if __name__ == '__main__':

    #Extraccion y Transformacion de datos
    df_semaforo = ExtractSheet().extract_sheet()


    df_transformado = Transform().transform_data(df_semaforo)


    #Almacenado en BDD
    instancia_bdd = Database(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    instancia_bdd.load_data(df_transformado)

    print("* Los indicadores de SEMAFORO han sido cargados")

    