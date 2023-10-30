from readSheets import readSheets
from connect_db import connect_db

#Datos de la base de datos
host = '172.17.16.157'
user = 'team-datos'
password = 'HCj_BmbCtTuCv5}'
database = 'ipecd_economico'


if __name__ == "__main__":
    df = readSheets().tratar_datos()
    print(df)
    connect_db().connect(df, host, user, password, database)


    