from censos_prueba import  homePage

#Datos de la base de datos
host = '172.17.16.177'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

if __name__ == '__main__':
    homePage().construir_df_estimaciones(host, user, password, database)