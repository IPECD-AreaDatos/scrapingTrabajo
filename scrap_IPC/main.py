from transform_region import TransformRegiones
from carga_db import conexcionBaseDatos
import os
import sys
from homePage import HomePage
from scrapingTrabajo.scrap_IPC.correo import Correo

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
instancia_credenciales = Credenciales('datalake_economico')
instancia_credenciales2 = Credenciales('dwh_economico')


if __name__ == '__main__':

    #Descargar EXCEL - Tambien almacenamos las rutas que usaremos
    home_page = HomePage()    
    home_page.descargar_archivo()
    
    # Armado del df de ipc variaciones
    instancia_transform = TransformRegiones(instancia_credenciales.host, instancia_credenciales.user, instancia_credenciales.password, instancia_credenciales.database)
    df = instancia_transform.main()

    #Creamos instancia de BDD y realizamos verficacion de carga. Si hay carga, la bandera sera True, sino False
    instancia_bdd = conexcionBaseDatos(instancia_credenciales.host, instancia_credenciales.user, instancia_credenciales.password, instancia_credenciales.database).conectar_bdd()
    bandera = instancia_bdd.main(df)
