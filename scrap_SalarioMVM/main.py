from homePage import HomePage
from conexion_bdd import conexionBaseDatos
import pandas as pd

df = pd.DataFrame()

instancia = HomePage()
instancia.descargar_archivo()
df = instancia.tratamiento_df()


#Conexion con BASE DE DATOS

#Datos de la base de datos
host = '172.17.16.157'
user = 'team-datos'
password = 'HCj_BmbCtTuCv5}'
database = 'ipecd_economico'

conexion = conexionBaseDatos(host,user,password,database)

conexion.conectar_bdd()

conexion.cargar_datos(df)


