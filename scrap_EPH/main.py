import os
import sys
from readSheets import readSheets
from connect_db import connect_db

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales = Credenciales('dwh_sociodemografico')


if __name__ ==  "__main__":

    df = readSheets().leer_datos_tasas()
    connect_db().connect(df, credenciales.host, credenciales.user, credenciales.password, credenciales.database)