import os
import sys
from conect_bdd import conexcionBaseDatos
from extract import Extraccion
from transform import Transformacion


# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales = Credenciales('datalake_economico')

if __name__ == "__main__":
    print("Las credenciales son: ", credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    extraer = Extraccion()
    extraer.descargar_archivo()
    df_combustible = Transformacion().crear_df()
    print("salida df")
    conexion = conexcionBaseDatos(credenciales.host, credenciales.user, credenciales.password, credenciales.database).conectar_bdd()
    conexion.cargaBaseDatos(df_combustible)