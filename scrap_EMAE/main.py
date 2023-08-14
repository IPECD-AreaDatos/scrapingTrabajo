from homePage import HomePage
import os
import pandas as pd
#from homePage import HomePage
from cargaIndice import cargaIndice

#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'


if __name__ == '__main__':
    
    #Obtencion del archivo
    url = HomePage()
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')
    file_path = os.path.join(ruta_carpeta_files, 'EMAE.xls')

    #Inicializamos el data frame y la listas de datos
    df = pd.DataFrame() 
    lista_fechas= list()
    lista_SectorProductivo = list()
    lista_valores= list() 
    
    cargaIndice().loadXLSIndiceEMAE(file_path, lista_fechas, lista_SectorProductivo, lista_valores, host, user, password, database)
    #conexionBaseDatos().cargaBaseDatos(lista_fechas, lista_SectorProductivo, lista_valores, host, user, password, database)
    