from homePage import HomePage
from loadXLS2_1 import LoadXLS2_1
from loadXLS2_2 import LoadXLS2_2
from loadXLS5_1 import LoadXLS5_1
from loadXLS5_2 import LoadXLS5_2
from calculoTotalNacion import calculoTotalNacion

#Datos de la base de datos
host = 'localhost'
user = 'root'
password = 'Estadistica123'
database = 'prueba1'

if __name__ == '__main__':
   url = HomePage()
   file_path = "C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scrap_SIPA\\files\\SIPA.xlsx"
   #file_path = "D:\\Users\\Pc-Pix211\\Desktop\\scrapingTrabajo\\scrap_SIPA\\files\\SIPA.xlsx"
   print("---->", file_path)
   print("El archivo se descargo correctamente")
   print("-------------------------------------------")
   print("------CARGA DE DATOS DE LA TABLA DE SIPA-----")
   LoadXLS2_1().loadInDataBase(file_path, host, user, password, database)
   LoadXLS2_2().loadInDataBase(file_path, host, user, password, database)
   LoadXLS5_1().loadInDataBase(file_path, host, user, password, database)
   LoadXLS5_2().loadInDataBase(file_path, host, user, password, database)
   calculoTotalNacion().loadInDataBase(host, user, password, database)
   
   