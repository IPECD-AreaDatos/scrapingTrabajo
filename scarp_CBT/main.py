from homePage import HomePage

#Datos de la base de datos
host = '192.168.0.101'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

if __name__ == '__main__':
    home_page = HomePage()
    home_page.descargar_archivo_CBT()
    home_page.descargar_archivo_Pobreza()
    