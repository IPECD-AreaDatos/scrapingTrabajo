from readSheets import readSheets
from connect_db import connect_db
from homePage_IPI import HomePage_IPI
from database_ipi import Database_ipi
#Datos de la base de datos
host = '172.17.22.23'
user = 'team-datos'
password = 'HCj_BmbCtTuCv5}'
database = 'ipecd_economico'


if __name__ == "__main__":
    df = readSheets().tratar_datos()
    print(df)
    connect_db().connect(df, host, user, password, database)

 
    home_page = HomePage_IPI()
    df_ipi = home_page.construir_df()

    
    Database_ipi().cargar_datos(host, user, password, database,df_ipi)

