from homePage import HomePage
from conexion_bdd import conexionBaseDatos
import pandas as pd

df = pd.DataFrame()

#Obtener datos 


instancia = HomePage()
instancia.descargar_archivo()
df = instancia.tratamiento_df()


#Conexion con BASE DE DATOS

#Datos de la base de datos
host = '192.168.0.101'
user = 'Ivan' 
password = 'Estadistica123'
database = 'prueba1'

conexion = conexionBaseDatos(host,user,password,database)

conexion.conectar_bdd()

conexion.cargar_datos(df)


