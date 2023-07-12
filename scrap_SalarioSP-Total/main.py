from homePage import HomePage
from loadCSVData_SP import loadCSVData_SP
from loadCSVData_Total import loadCSVData_Total

#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

if __name__ == '__main__':
    home_page = HomePage()
    home_page.descargar_archivo()
    loadCSVData_SP().loadInDataBase(host, user, password, database)
    loadCSVData_Total().loadInDataBase(host, user, password, database)