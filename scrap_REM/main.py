
import sys
import os
import pandas as pd
from extract import Extract 
from transform_precios_minoristas import Transform1 
from transform_cambio_nominal import Transform2 

from load import conexcionBaseDatos

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, "..", "Credenciales_folder")
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Importar las credenciales

# Documentación:
# - Importaciones necesarias para el script.
# - Obtención de las credenciales necesarias para la conexión a bases de datos.
# - Bases con el mismo nombre / local_... para las del servidor local
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales_datalake_economico = Credenciales("datalake_economico")

if __name__ == "__main__":
    #extract = Extract()
    #extract.descargar_archivo()

    df_rem_precios_minoristas = Transform1().crear_df_precios_minoristas()
    df_rem_cambio_nominal = Transform2().crear_df_cambio_nominal()
    print(df_rem_precios_minoristas)
    print(df_rem_cambio_nominal)

    
    credenciales = conexcionBaseDatos(credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database).conectar_bdd()
    credenciales.cargaBaseDatos(df_rem_precios_minoristas)
    credenciales.cargaBaseDatos2(df_rem_cambio_nominal)
