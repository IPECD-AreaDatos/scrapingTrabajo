from homePage import HomePage
from conexion_bdd import conexionBaseDatos
import pandas as pd
import sys
import os

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))


df = pd.DataFrame()

instancia = HomePage()
instancia.descargar_archivo()
df = instancia.tratamiento_df()

#Conexion con BASE DE DATOS
conexion = conexionBaseDatos(host_dbb,user_dbb,pass_dbb,dbb_datalake)

conexion.conectar_bdd()

conexion.cargar_datos(df)


