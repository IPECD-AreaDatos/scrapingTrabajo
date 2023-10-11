from homePage import HomePage
from loadCSVData import LoadCSVData
from loadCSVPuestosTotal import LoadCSVDataPuestosTotal

#Datos de la base de datos
host = '172.17.16.177'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

if __name__ == '__main__':
    home_page = HomePage()
    home_page.descargar_archivo()
    LoadCSVData().loadInDataBase(host, user, password, database)
    LoadCSVDataPuestosTotal().loadInDataBase(host, user, password, database)
    
    
   #file_path_Departamentos="D:\\Users\\Pc-Pix211\\Desktop\\scrapingTrabajo\\scrap_PuestosTrabajoSP\\files\\csv\\diccionario_cod_depto.csv"
   #LoadCSVDataDepartamentos().loadInDataBase(file_path_Departamentos, host, user, password, database)