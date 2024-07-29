
import sys
import os
from extract import Extract 
from transform_precios_minoristas import Transform
from load import conexionBaseDatos
from pandas import to_datetime

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, "..", "Credenciales_folder")

# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)

#Obtención de las credenciales necesarias para la conexión a bases de datos, usando una instancia.
from credenciales_bdd import Credenciales

# Después puedes crear una instancia de Credenciales
credenciales_datalake_economico = Credenciales("datalake_economico")


if __name__ == "__main__":

    #Extraccion de archivos
    Extract().descargar_archivo()

    #Obtencion de DATAFRAMES 
    instancia_transform = Transform()
    df_rem_precios_minoristas = instancia_transform.get_historico_precios_minoristas()
    df_rem_cambio_nominal = instancia_transform.get_historico_cambio_nominal()

    #Carga de datos
    instancia_load = conexionBaseDatos(credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database)
    instancia_load.main(df_rem_precios_minoristas,df_rem_cambio_nominal)
