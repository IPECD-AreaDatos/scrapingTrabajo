from homePage import HomePage
from loadXLS2_1 import LoadXLS2_1

#Datos de la base de datos
host = 'localhost'
user = 'root'
password = 'Estadistica123'
database = 'prueba1'

if __name__ == '__main__':
   url = HomePage()
   file_path = "C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scrap_SIPA\\files\\SIPA.xlsx"
   print("---->", file_path)
   print("El archivo se descargo correctamente")
   print("-------------------------------------------")
   print("------CARGA DE DATOS DE LA TABLA DE SIPA-----")
   #LoadXLS2_1().loadInDataBase(file_path, host, user, password, database)
   
   