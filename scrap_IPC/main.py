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

#Datos de la base de datos
host = 'localhost'
user = 'root'
password = 'Estadistica123'
database = 'prueba1'


if __name__ == '__main__':
   url = HomePage()
   file_path = "C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scrap_IPC\\files\\xls\\archivo.xls"
   print("---->", file_path)
   
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
   