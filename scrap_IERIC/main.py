from downloadArchive import downloadArchive
from readFile import readFile
from uploadDataInDataBase import uploadDataInDataBase
from downloadArchive import downloadArchive
from readFile2 import readFile2
import os
import sys

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


if __name__=='__main__':
    print("Las credenciales son: ", credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database)
    #downloadArchive().descargar_archivo()
    df = readFile().read_file()
    df2 = readFile2().create_df()
    credenciales = uploadDataInDataBase(credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database).conectar_bdd()
    credenciales.cargaBaseDatos(df2)
    credenciales.load_data(df)