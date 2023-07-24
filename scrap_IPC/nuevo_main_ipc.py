
from homePage import HomePage
from armadoXLSDataNacion import LoadXLSDataNacion
from armadoXLSDataGBA import LoadXLSDataGBA
from armadoXLSDataPampena import LoadXLSDataPampena
from armadoXLSDataNOA import LoadXLSDataNOA
from armadoXLSDataNEA import LoadXLSDataNEA
from armadoXLSDatacuyo import LoadXLSDataCuyo
from armadoXLSDataPatagonia import LoadXLSDataPatagonia
import os
import pandas as pd

"""
--- PASOS ---

1) Construir EXCEl
2) Verificar fechas para actualizacion

"""
#Listas a tratar durante el proceso
lista_fechas = list()
lista_regiones = list()
lista_subdivision = list()
lista_valores = list()

df = pd.DataFrame()

#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'
valor_region = 1

if __name__ == '__main__':
    #Descargar EXCEL - Tambien almacenamos las rutas que usaremos
    url = HomePage()
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')
    file_path = os.path.join(ruta_carpeta_files, 'archivo.xls')
    valoresDeIPC = [
      LoadXLSDataNacion,
      LoadXLSDataGBA,
      LoadXLSDataPampeana,
      LoadXLSDataNOA,
      LoadXLSDataNEA,
      LoadXLSDataCuyo,
      LoadXLSDataPatagonia,
    ]
    for regiones in valoresDeIPC:
      print("Valor region: ", valor_region)
      regiones().loadInDataBase(file_path, lista_fechas, lista_regiones, valor_region ,lista_subdivision, lista_valores)
    
    df['fecha'] = lista_fechas
    df['regiones'] = lista_regiones
    df['subdivision'] = lista_subdivision
    df['valores'] = lista_valores
    for i in df.values:
        print(i)