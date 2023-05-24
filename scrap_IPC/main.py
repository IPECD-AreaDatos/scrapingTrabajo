from homePage import HomePage
from fileManager import FileManager
from loadCSVData import LoadCSVData

if __name__ == '__main__':
   url =  HomePage().getDownloadUrl()
   #file_name = FileManager(url).downloadCSV()
   #print("---->", file_name)
   #file_path="C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scrap_IPC\\files\\xls\\{}".format(file_name)
   #print("---->", file_path)
   #LoadCSVData().loadInDataBase(file_path)
   
