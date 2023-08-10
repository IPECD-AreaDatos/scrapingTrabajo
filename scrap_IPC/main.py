from homePage import HomePage
from armadoXLSDataGBA import LoadXLSDataGBA
from armadoXLSDataPampeana import LoadXLSDataPampeana
from armadoXLSDataNOA import LoadXLSDataNOA
from armadoXLSDataNEA import LoadXLSDataNEA
from armadoXLSDataCuyo import LoadXLSDataCuyo
from armadoXLSDataPatagonia import LoadXLSDataPatagonia
from conexionBaseDatos import conexionBaseDatos
import os
import pandas as pd
import mysql.connector
import datetime


#Listas a tratar durante el proceso
lista_fechas = list()
lista_region = list()
lista_categoria = list()
lista_division = list()
lista_subdivision = list()
lista_valores = list()



#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

valor_region = 2

if __name__ == '__main__':
    #Descargar EXCEL - Tambien almacenamos las rutas que usaremos
    url = HomePage()
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')
    file_path = os.path.join(ruta_carpeta_files, 'IPC_Desagregado.xls')
    valoresDeIPC = [
      #LoadXLSDataNacion,
      LoadXLSDataGBA,
      LoadXLSDataPampeana,
      LoadXLSDataNOA,
      LoadXLSDataNEA,
      LoadXLSDataCuyo,
      LoadXLSDataPatagonia,
    ]
    for regiones in valoresDeIPC:
      print("Valor region: ", valor_region)
      regiones().loadInDataBase(file_path, valor_region, lista_fechas, lista_region,  lista_categoria, lista_division, lista_subdivision, lista_valores)
      valor_region = valor_region + 1
    conexionBaseDatos().cargaBaseDatos(lista_fechas, lista_region, lista_categoria, lista_division, lista_subdivision, lista_valores, host, user, password, database)




