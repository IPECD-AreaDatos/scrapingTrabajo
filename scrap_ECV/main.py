import os
import sys
from readSheetsTasas import readSheetsTasas
from connect_db import connect_db
from readSheetsTrabajo import readSheetsTrabajo
from readSheetsTrabajoQuintiles import readSheetsTrabajoQuintiles

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
    print("Las credenciales son", credenciales.host,credenciales.user,credenciales.password,credenciales.database)
    #df_tasas = readSheetsTasas().leer_datos_tasas()
    #connect_db().connect_db_tasas(df_tasas, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    #df_trabajo = readSheetsTrabajo().leer_datos_trabajo()
    #connect_db().connect_db_trabajo(df_trabajo, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    df_trabajo_quintiles = readSheetsTrabajoQuintiles().leer_datos_trabajo_quintiles()
    connect_db().connect_db_trabajo_quintiles(df_trabajo_quintiles, credenciales.host, credenciales.user, credenciales.password, credenciales.database)