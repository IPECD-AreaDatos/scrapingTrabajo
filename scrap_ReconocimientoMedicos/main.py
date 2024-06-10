import os
import sys
from readDataBaseMedicos import readDataBaseMedicos
from loadDataBaseNuestra import loadDataBaseNuestra


#Configuracion de la ruta de credenciales
# Obtiene la ruta absoluta al directorio donde reside el script actual.
script_dir = os.path.dirname(os.path.abspath(__file__))
# Crea una ruta al directorio 'Credenciales_folder' que se supone está un nivel arriba en la jerarquía de directorios.
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)

# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales = Credenciales('reconocimientos_medicos')
credenciales_nuestras = Credenciales('dwh_sociodemografico')

if __name__ == "__main__":
    print("Las credenciales son", credenciales.host,credenciales.user,credenciales.password,credenciales.database)
    conexion = readDataBaseMedicos(credenciales.host, credenciales.user, credenciales.password, credenciales.database).conectar_bdd()
    df = conexion.readDataBaseMedicos()
    conexion.cerrar_conexion()
    print(df)
    instancia_bdd = loadDataBaseNuestra(credenciales_nuestras.host, credenciales_nuestras.user, credenciales_nuestras.password, credenciales_nuestras.database)
    instancia_bdd.loadDataBaseNuestra(df)
    instancia_bdd.cerrar_conexion()
    