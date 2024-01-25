from extract import HomePage
import os
import pandas as pd
#from homePage import HomePage
from cargaIndice import cargaIndice

#Datos de la base de datos
host = '172.17.22.23'
user = 'team-datos'
password = 'HCj_BmbCtTuCv5}'
database = 'ipecd_economico'


if __name__ == '__main__':
    
    #Obtencion del archivo
    url = HomePage()
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')
    
    file_path = os.path.join(ruta_carpeta_files, 'EMAE.xls')
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')
    file_path_variacion = os.path.join(ruta_carpeta_files, 'EMAEVAR.xls')

    #Inicializamos el data frame y la listas de datos
    df = pd.DataFrame() 
    lista_fechas= list()
    lista_SectorProductivo = list()
    lista_valores= list() 

    cargaIndice().loadXLSVariacionEMAE(file_path_variacion, lista_fechas, host, user, password, database)
    cargaIndice().loadXLSIndiceEMAE(file_path, lista_fechas, lista_SectorProductivo, lista_valores, host, user, password, database)
