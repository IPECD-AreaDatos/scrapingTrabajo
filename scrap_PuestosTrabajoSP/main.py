from homePage import HomePage
from loadCSVData import LoadCSVData
from loadCSVPuestosTotal import LoadCSVDataPuestosTotal

#Datos de la base de datos
host = '172.17.16.157'
user = 'team-datos'
password = 'HCj_BmbCtTuCv5}'
database = 'ipecd_economico'

if __name__ == '__main__':
    home_page = HomePage()
    home_page.descargar_archivo()
    LoadCSVData().loadInDataBase(host, user, password, database)
    LoadCSVDataPuestosTotal().loadInDataBase(host, user, password, database)