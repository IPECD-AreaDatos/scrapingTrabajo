from homePage import HomePage
from fileManager import FileManager
from loadCSVData import LoadCSVData
from loadCSVDataDepartamentos import LoadCSVDataDepartamentos

if __name__ == '__main__':
   url =  HomePage().getDownloadUrl()
   file_name = FileManager(url).downloadCSV()
   print("---->", file_name)
   #file_path_Departamentos="D:\\Users\\Pc-Pix211\\Desktop\\scrapingTrabajo\\scrap_PuestosTrabajoSP\\files\\csv\\diccionario_cod_depto.csv"
   #LoadCSVDataDepartamentos().loadInDataBase(file_path_Departamentos)
   file_path="C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scraper\\files\\csv\\{}".format(file_name)
   print("---->", file_path)
   LoadCSVData().loadInDataBase(file_path)
   
   
