from censos_prueba import  homePage
import os
import sys 

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
    homePage().construir_df_estimaciones(credenciales.host, credenciales.user, credenciales.password, credenciales.database)