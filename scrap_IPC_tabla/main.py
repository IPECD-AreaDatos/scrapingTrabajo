"""
Archivo destinado a construir la tabla correspondiente a Variaciones de distintos tipos de IPC.

IPC's que se tienen cuenta:

    - IPC a nivel NACIONAL
    - IPC NEA
    - IPC CABA
    - IPC ONLINE
    - IPC REM Precios minorista

    * Todos estos datos se extraen de "Datalake_economico"

Al ser una tabla analitica, la almacenaremos en dwh_economico

"""

from create_df import ExtractDataBDD
from loaddb import Load
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
cred_dl_economico = Credenciales("datalake_economico")

if __name__ == '__main__':
    
    #Creamos la intancia para buscar los datos
    instancia = ExtractDataBDD(cred_dl_economico.host, cred_dl_economico.user, cred_dl_economico.password, cred_dl_economico.database)
    df = instancia.main()#--> Obtenemos los datos en un DF

    #Cargamos la BDD --> En este caso indicamos que la base de datos sera 'dwh_economico' | No creamos instancia, ya que solo cambiariamos esta cadena
    credenciales = Load(cred_dl_economico.host, cred_dl_economico.user, cred_dl_economico.password, 'dwh_economico')
    credenciales.main(df)


