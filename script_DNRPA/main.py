from loadHTML_TablaAutoInscripcionNacion import loadHTML_TablaAutoInscripcionNacion
from loadHTML_TablaMotoInscripcionNacion import loadHTML_TablaMotoInscripcionNacion
from loadHTML_TablaAutoInscripcionCorrientes import loadHTML_TablaAutoInscripcionCorrientes


#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

# Lista de tablas a cargar
inscripcion = [
    loadHTML_TablaAutoInscripcionNacion,
    loadHTML_TablaMotoInscripcionNacion,
    loadHTML_TablaAutoInscripcionCorrientes
]

if __name__ == '__main__':
    for registroPropiedadAutomotor in inscripcion:
        registroPropiedadAutomotor().loadInDataBase(host, user, password, database)