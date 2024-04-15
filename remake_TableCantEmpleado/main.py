import sys
import os
from load_table_provincias import load_table_provincia

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, "..", "Credenciales_folder")
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Importar las credenciales

# Documentación:
# - Importaciones necesarias para el script.
# - Obtención de las credenciales necesarias para la conexión a bases de datos.
# - Bases con el mismo nombre / local_... para las del servidor local
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales_dwh_economico = Credenciales("dwh_economico")
credenciales_ipecd_economico = Credenciales("ipecd_economico")

# Instancia encargada

if __name__ == "__main__":
    credenciales = load_table_provincia(credenciales_ipecd_economico.host, credenciales_ipecd_economico.user, credenciales_ipecd_economico.password, credenciales_ipecd_economico.database).conectar_bdd()
    results = credenciales.read_database()
    for fecha, suma in results.items():
        print(f"Total sum for {fecha}: {suma}")