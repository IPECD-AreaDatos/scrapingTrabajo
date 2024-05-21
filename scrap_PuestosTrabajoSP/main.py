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
credenciales = Credenciales('datalake_economico')

if __name__ == '__main__':
    #Carga de documento de departamentos
    #LoadCSVDataDepartamentos().loadInDataBase(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    #home_page = HomePage()
    #home_page.descargar_archivo()
    instancia = LoadCSVData(host=credenciales.host, user=credenciales.user, password=credenciales.password, database=credenciales.database)
    instancia_privado = LoadCSVDataPuestosTotal(host=credenciales.host, user=credenciales.user, password=credenciales.password, database=credenciales.database)
    instancia.loadInDataBase()
    instancia_privado.loadInDataBase()