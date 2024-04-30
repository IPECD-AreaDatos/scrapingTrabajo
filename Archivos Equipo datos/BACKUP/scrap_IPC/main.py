from homePage import HomePage
from loadXLSDataNEA import LoadXLSDataNEA
from loadXLSDataNacion import LoadXLSDataNacion
from loadXLSDataGBA import LoadXLSDataGBA
from loadXLSDataPampeana import LoadXLSDataPampeana
from loadXLSDataNoroeste import LoadXLSDataNoroeste
from loadXLSDataCuyo import LoadXLSDataCuyo
from loadXLSDataPatagonia import LoadXLSDataPatagonia
from armadoVariacionIntermensualNacion import armadoVariacionIntermensualNacion
from armadoVariacionIntermensualNEA import armadoVariacionIntermensualNEA
from armadoVariacionIntermensualPampeana import armadoVariacionIntermensualPampeana
from armadoVariacionIntermensualPatagonia import armadoVariacionIntermensualPatagonia
from armadoVariacionIntermensualNoroeste import armadoVariacionIntermensualNoroeste
from armadoVariacionIntermensualGba import armadoVariacionIntermensualGba
from armadoVariacionIntermensualCuyo import armadoVariacionIntermensualCuyo
from armadoVariacionInteranualNacion import armadoVariacionInteranualNacion
from armadoVariacionInteranualCuyo import armadoVariacionInteranualCuyo
from armadoVariacionInteranualGBA import armadoVariacionInteranualGBA
from armadoVariacionInteranualNoroeste import armadoVariacionInteranualNoroeste
from armadoVariacionInteranualPampeana import armadoVariacionInteranualPampeana
from armadoVariacionInteranualPatagonia import armadoVariacionInteranualPatagonia
from armadoVariacionInteranualNea import armadoVariacionInteranualNEA
import os

#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'


if __name__ == '__main__':
   url = HomePage()
   directorio_actual = os.path.dirname(os.path.abspath(__file__))
   ruta_carpeta_files = os.path.join(directorio_actual, 'files')
   file_path = os.path.join(ruta_carpeta_files, 'archivo.xls')
   print("---->", file_path)
   print("-------------------------------------------")
   print("------CARGA DE DATOS DE LA TABLA DE IPC------")
   valoresDeIPC = [
      LoadXLSDataNEA,
      LoadXLSDataNacion,
      LoadXLSDataGBA,
      LoadXLSDataPampeana,
      LoadXLSDataNoroeste,
      LoadXLSDataCuyo,
      LoadXLSDataPatagonia
    ]
   for regiones in valoresDeIPC:
      regiones().loadInDataBase(file_path, host, user, password, database)
   print("-------------------------------------------")
   print("------CALCULO DE VARIACION INTERMENSUAL------")
   calculo_intermensual = [
      armadoVariacionIntermensualNacion,
      armadoVariacionIntermensualNEA,
      armadoVariacionIntermensualPampeana,
      armadoVariacionIntermensualPatagonia,
      armadoVariacionIntermensualNoroeste,
      armadoVariacionIntermensualGba,
      armadoVariacionIntermensualCuyo
    ]
   for regiones in calculo_intermensual:
        regiones().calculoVariacion(host, user, password, database)
   print("-------------------------------------------")
   print("------CALCULO DE VARIACION INTERANUAL------")
   calculo_interanual = [
      armadoVariacionInteranualNacion,
      armadoVariacionInteranualCuyo,
      armadoVariacionInteranualGBA,
      armadoVariacionInteranualNoroeste,
      armadoVariacionInteranualPampeana,
      armadoVariacionInteranualPatagonia,
      armadoVariacionInteranualNEA
    ]
   for regiones in calculo_interanual:
      regiones().calculoVariacion(host, user, password, database)
   