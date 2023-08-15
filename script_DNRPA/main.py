from loadHTML_TablaAutoInscripcionNacion import loadHTML_TablaAutoInscripcionNacion
from loadHTML_TablaMotoInscripcionNacion import loadHTML_TablaMotoInscripcionNacion
from loadHTML_TablaAutoInscripcionCorrientes import loadHTML_TablaAutoInscripcionCorrientes
from loadHTML_TablaMotoInscripcionCorrientes import loadHTML_TablaMotoInscripcionCorrientes
from loadHTML_TablaParqueActivoNacion import loadHTML_TablaParqueActivoNacion


#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

# Lista de tablas a cargar
inscripcion = [
    loadHTML_TablaAutoInscripcionNacion,
    loadHTML_TablaMotoInscripcionNacion,
    loadHTML_TablaAutoInscripcionCorrientes,
    loadHTML_TablaMotoInscripcionCorrientes,
    loadHTML_TablaParqueActivoNacion

]

if __name__ == '__main__':
    for registroPropiedadAutomotor in inscripcion:
        registroPropiedadAutomotor().loadInDataBase(host, user, password, database)
        
        
