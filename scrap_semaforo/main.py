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

    #Extraccion y Transformacion de datos semoforo INTERANUAL
    df_semaforo_interanual = ExtractSheet().extract_sheet_internual()
    df_interanual_transformado = Transform().transform_data(df_semaforo_interanual)
    print(df_interanual_transformado)

    #Extraccion y Transformacion de datos semoforo INTERMENSUAL
    df_semaforo_intermensual = ExtractSheet().extract_sheet_intermensual()
    df_intermensual_transformado = Transform().transform_data(df_semaforo_intermensual)
    print(df_intermensual_transformado)

    #Almacenado en BDD
    instancia_bdd = Database(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    instancia_bdd.load_data(df_interanual_transformado, df_intermensual_transformado)

    print("* Los indicadores de SEMAFORO han sido cargados")

    