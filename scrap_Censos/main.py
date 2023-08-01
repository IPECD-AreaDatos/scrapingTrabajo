from censos_prueba import homePage

#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'


#Creamos intancia
instancia = homePage() 

#Descarga de archivos
instancia.descargar_archivos()

#Construir estimaciones


