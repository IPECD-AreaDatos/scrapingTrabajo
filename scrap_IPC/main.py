from homePage import HomePage
from fileManager import FileManager
from loadXLSDataNEA import LoadXLSDataNEA
from loadXLSDataNacion import LoadXLSDataNacion
from loadXLSDataGBA import LoadXLSDataGBA
from loadXLSDataPampeana import LoadXLSDataPampeana
from loadXLSDataNoroeste import LoadXLSDataNoroeste
from loadXLSDataCuyo import LoadXLSDataCuyo
from loadXLSDataPatagonia import LoadXLSDataPatagonia

if __name__ == '__main__':
   url =  HomePage()
   #file_name = FileManager(url).downloadCSV()
   file_path="C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scrap_IPC\\files\\xls\\archivo.xls"
   print("---->", file_path)
   LoadXLSDataNEA().loadInDataBase(file_path)
   LoadXLSDataNacion().loadInDataBase(file_path)
   LoadXLSDataGBA().loadInDataBase(file_path)
   LoadXLSDataPampeana().loadInDataBase(file_path)
   LoadXLSDataNoroeste().loadInDataBase(file_path)
   LoadXLSDataCuyo().loadInDataBase(file_path)
   LoadXLSDataPatagonia().loadInDataBase(file_path)
   
   
