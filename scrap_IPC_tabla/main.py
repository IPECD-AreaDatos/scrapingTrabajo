"""
Archivo destinado a construir la tabla correspondiente a Variaciones de distintos tipos de IPC.

IPC's que se tienen cuenta:

    - IPC a nivel NACIONAL
    - IPC NEA
    - IPC CABA
    - IPC ONLINE
    - IPC REM Precios minorista

"""

from create_df import ExtractDataBDD
from loaddb import conexcionBaseDatos
from df_acumulado import Create_Df_Acum
import os
import sys


# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, "..", "Credenciales_folder")

# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)

# Importar las credenciales
from credenciales_bdd import Credenciales

#instancia de Credenciales
credenciales_datalake_economico = Credenciales("datalake_economico")

if __name__ == '__main__':
    
    credenciales = ExtractDataBDD(credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database)

    df = credenciales.extraer_datos()

    sys.exit()

    credenciales = conexcionBaseDatos(credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database).conectar_bdd()
    credenciales.cargaBaseDatos(df)

    credenciales2 = Create_Df_Acum(credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database).conectar_bdd()

    df_acum = credenciales2.armarAcum()

    credenciales2 = conexcionBaseDatos(credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database).conectar_bdd()
    credenciales2.cargaBaseDatos2(df_acum)
