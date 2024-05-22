import os
import sys
from downloadArchive import downloadArchive
from conexcionBaseDatos import conexcionBaseDatos
from readFile2 import readFile2

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales = Credenciales('datalake_economico')

if __name__=='__main__':
    print("Las credenciales son: ", credenciales.host, credenciales.user, credenciales.password, credenciales.database)

    #downloadArchive().descargar_archivo()
    df = readFile2().create_df()

    conexion = conexcionBaseDatos(credenciales.host, credenciales.user, credenciales.password, credenciales.database).conectar_bdd()
    conexion.cargaBaseDatos(df)