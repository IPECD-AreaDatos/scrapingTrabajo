from homePage import HomePage
from loadXLS2_1 import LoadXLS2_1
from loadXLS2_2 import LoadXLS2_2
from loadXLS5_1 import LoadXLS5_1
from loadXLS5_2 import LoadXLS5_2
from calculoTotalNacion import calculoTotalNacion
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
   file_path = os.path.join(ruta_carpeta_files, 'SIPA.xlsx')
   print("---->", file_path)
   print("El archivo se descargo correctamente")
   print("-------------------------------------------")
   print("------CARGA DE DATOS DE LA TABLA DE SIPA-----")
   LoadXLS2_1().loadInDataBase(file_path, host, user, password, database)
   LoadXLS2_2().loadInDataBase(file_path, host, user, password, database)
   LoadXLS5_1().loadInDataBase(file_path, host, user, password, database)
   LoadXLS5_2().loadInDataBase(file_path, host, user, password, database)
   calculoTotalNacion().loadInDataBase(host, user, password, database)
   
   