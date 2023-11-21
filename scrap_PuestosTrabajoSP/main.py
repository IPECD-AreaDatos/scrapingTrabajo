from homePage import HomePage
from loadCSVData import LoadCSVData
from loadCSVPuestosTotal import LoadCSVDataPuestosTotal
import os
import sys

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)


from credenciales_bdd import Credenciales


credenciales = Credenciales()

if __name__ == '__main__':
    home_page = HomePage()
    home_page.descargar_archivo()
    LoadCSVData().loadInDataBase(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    LoadCSVDataPuestosTotal().loadInDataBase(credenciales.host, credenciales.user, credenciales.password, credenciales.database)