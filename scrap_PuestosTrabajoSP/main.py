from homePage import HomePage
from loadCSVData import LoadCSVData
from loadCSVPuestosTotal import LoadCSVDataPuestosTotal
import os
import sys
from loadCSVDataDepartamentos import LoadCSVDataDepartamentos

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales = Credenciales('ipecd_economico')

if __name__ == '__main__':
    #Carga de documento de departamentos
    #LoadCSVDataDepartamentos().loadInDataBase(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    home_page = HomePage()
    home_page.descargar_archivo()
    LoadCSVData().loadInDataBase(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    LoadCSVDataPuestosTotal().loadInDataBase(credenciales.host, credenciales.user, credenciales.password, credenciales.database)