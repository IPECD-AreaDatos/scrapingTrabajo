from homePage import HomePage
from fileManager import FileManager
from loadXLSData import LoadXLSData

if __name__ == '__main__':
   #url =  HomePage()
   #file_name = FileManager(url).downloadCSV()
   file_path="C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scrap_IPC\\files\\xls\\archivo.xls"
   print("---->", file_path)
   LoadXLSData().loadInDataBase(file_path)
   
