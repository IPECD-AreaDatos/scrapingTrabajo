import os
import sys
from readGoogleSheets import readGoogleSheets
from connect_db import DatabaseManager

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
credenciales = Credenciales('ipecd_economico')
credenciales2 = Credenciales('datalake_economico')


if __name__ == "__main__":
    #-----IPICORR--------
    df_ipicorr = readGoogleSheets().tratar_datos()
    DatabaseManager(credenciales.host, credenciales.user, credenciales.password, credenciales.database).update_database_with_new_data(df_ipicorr)
    DatabaseManager(credenciales2.host, credenciales2.user, credenciales2.password, credenciales2.database).update_database_with_new_data(df_ipicorr)