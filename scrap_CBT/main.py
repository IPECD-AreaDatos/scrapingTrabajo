from homePage_CBT import HomePageCBT
from homePage_Pobreza import HomePagePobreza
from loadXLSDataCBT import loadXLSDataCBT
from connectionDataBase import connection_db

#Datos de la base de datos
host = '172.17.16.157'
user = 'team-datos'
password = 'HCj_BmbCtTuCv5}'
database = 'ipecd_economico'

if __name__ == '__main__':
    home_page_CBT = HomePageCBT()
    home_page_CBT.descargar_archivo()
    
    home_page_Pobreza= HomePagePobreza()
    home_page_Pobreza.descargar_archivo()
    
    loadXLSDataCBT().readData()

    instancia = connection_db(host, user, password, database)
    instancia.carga_db()

    print("- Finalizacion de revison de CBT")