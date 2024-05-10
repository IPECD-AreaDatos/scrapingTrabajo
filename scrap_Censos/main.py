from censos_prueba import  homePage
import os
import sys 
from load_database import load_database

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
    df = homePage().construir_df_estimaciones(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    print(df)
    instancia_bdd = load_database(credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    instancia_bdd.load_data(df)
