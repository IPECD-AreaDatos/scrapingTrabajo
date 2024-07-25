"""
En esta ocacion fue necesario durante el proceso de extraccion de los datos
una transformacion, esto porque el tratado de la tabla de DNRPA requeria de 
varias operaciones para concatenar las tablas de diferentes años entre si.
"""

from exctract import ExtractDnrpa
from exctract_last_year import ExtractLast
from load import conexionBaseDatos
from load_last import conexionBaseDatosLast

import os , sys

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

    #Extraccion y TRANSFORMACION de datos
    df = ExtractLast().extraer_tablas()
    
    #Cargamos al datalake economico
    instancia_bdd = conexionBaseDatosLast(credenciales.host,credenciales.user,credenciales.password,credenciales.database)
    instancia_bdd.cargar_datalake(df)

