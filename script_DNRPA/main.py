from loadHTML_TablaAutoNacion import loadHTML_TablaAutoNacion

#Datos de la base de datos
host = 'localhost'
user = 'root'
password = 'Estadistica123'
database = 'prueba1'

if __name__ == '__main__':
    loadHTML_TablaAutoNacion().loadInDataBase(host, user, password, database)