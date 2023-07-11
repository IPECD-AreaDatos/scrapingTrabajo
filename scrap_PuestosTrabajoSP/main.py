from homePage import HomePage
from fileManager import FileManager
from loadCSVData import LoadCSVData
from loadCSVDataDepartamentos import LoadCSVDataDepartamentos
import os

if __name__ == '__main__':
   url =  HomePage().getDownloadUrl()
   file_name = FileManager(url).downloadCSV()
   # Obtener la ruta del directorio actual (donde se encuentra el script)
   directorio_actual = os.path.dirname(os.path.abspath(__file__))

   # Construir la ruta de la carpeta "files" dentro del directorio actual
   ruta_carpeta_files = os.path.join(directorio_actual, 'files')

   # Construir la ruta completa del archivo CSV dentro de la carpeta "files"
   file_path = os.path.join(ruta_carpeta_files, file_name)
   print("---->", file_path)
   
   LoadCSVData().loadInDataBase(file_path)
   

   #file_path_Departamentos="D:\\Users\\Pc-Pix211\\Desktop\\scrapingTrabajo\\scrap_PuestosTrabajoSP\\files\\csv\\diccionario_cod_depto.csv"
   #LoadCSVDataDepartamentos().loadInDataBase(file_path_Departamentos)
