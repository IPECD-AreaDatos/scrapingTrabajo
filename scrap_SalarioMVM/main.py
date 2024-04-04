from homePage import HomePage
from conexion_bdd import conexionBaseDatos
import pandas as pd
import sys
import os

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)


from credenciales_bdd import Credenciales


credenciales = Credenciales('datalake_economico')


df = pd.DataFrame()

instancia = HomePage()
instancia.descargar_archivo()
df = instancia.tratamiento_df()

#Conexion con BASE DE DATOS
conexion = conexionBaseDatos(credenciales.host,credenciales.user,credenciales.password,credenciales.database)

conexion.conectar_bdd()

conexion.cargar_datos(df)


