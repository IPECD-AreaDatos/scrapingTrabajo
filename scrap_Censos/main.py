from censos_prueba import  homePage

#Datos de la base de datos
host = '172.17.16.157'
user = 'team-datos'
password = 'HCj_BmbCtTuCv5}'
database = 'ipecd_economico'

if __name__ == '__main__':
    homePage().construir_df_estimaciones(host, user, password, database)