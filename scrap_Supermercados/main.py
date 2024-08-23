#Importacion de Bibliotecas
import os
import sys
from Extraction_homePage import HomePage
from Transformation_super import Transformation_Data
from Load_super import conexionBaseDatos

from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))

def main():

    #Descarga del archivo
    HomePage().descargar_archivo()

    #Obtencion del dataframe con formato solicitado
    df = Transformation_Data().contruccion_df()

    #Validamos las fechas para conocer estimativamente en que rango de tiemp oestamos
    print(f"Fecha minima del supermercado: {min(df['fecha'])}")
    print(f"Fecha maxima del supermercado: {max(df['fecha'])}")

    #Almacenamos los datos
    conexionBaseDatos(host_dbb,user_dbb,pass_dbb,dbb_datalake).cargar_datos(df)


if __name__ == '__main__':

    main()

