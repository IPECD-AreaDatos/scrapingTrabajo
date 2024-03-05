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
    #df = readSheetsTasas().leer_datos_tasas()
    #connect_db().connectECVTasas(df, credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    readSheetsTrabajoQuintiles().leer_datos_trabajo_quintiles()