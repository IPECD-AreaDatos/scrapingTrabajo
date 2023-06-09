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

if __name__ == '__main__':
   url =  HomePage()
   file_path="C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scrap_IPC\\files\\xls\\archivo.xls"
   print("---->", file_path)
   LoadXLSDataNEA().loadInDataBase(file_path)
   LoadXLSDataNacion().loadInDataBase(file_path)
   LoadXLSDataGBA().loadInDataBase(file_path)
   LoadXLSDataPampeana().loadInDataBase(file_path)
   LoadXLSDataNoroeste().loadInDataBase(file_path)
   LoadXLSDataCuyo().loadInDataBase(file_path)
   LoadXLSDataPatagonia().loadInDataBase(file_path)
   armadoVariacionIntermensualNacion().calculoVariacion()
   armadoVariacionIntermensualNEA().calculoVariacion()
   armadoVariacionIntermensualPampeana().calculoVariacion()
   armadoVariacionIntermensualPatagonia().calculoVariacion()
   armadoVariacionIntermensualNoroeste().calculoVariacion()
   armadoVariacionIntermensualGba().calculoVariacion()
   armadoVariacionIntermensualCuyo().calculoVariacion()
   armadoVariacionInteranualNacion().calculoVariacion()   
   armadoVariacionInteranualCuyo().calculoVariacion()
   armadoVariacionInteranualGBA().calculoVariacion()
   armadoVariacionInteranualNoroeste().calculoVariacion()
   armadoVariacionInteranualPampeana().calculoVariacion()
   armadoVariacionInteranualPatagonia().calculoVariacion()
   armadoVariacionInteranualNEA().calculoVariacion()
   
   