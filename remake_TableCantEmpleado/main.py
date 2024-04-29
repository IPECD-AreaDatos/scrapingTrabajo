import sys
import os
from load_censo_total import load_censo_total
from load_sipa_valores import load_sipa_valores


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
credenciales_datalake_economico = Credenciales("datalake_economico")
# Instancia encargada
star_date = (2010)
end_year = (2024)

if __name__ == "__main__":
    #credenciales_censo = load_censo_total(credenciales_ipecd_economico.host, credenciales_ipecd_economico.user, credenciales_ipecd_economico.password, credenciales_ipecd_economico.database).conectar_bdd()
    #credenciales_censo.read_censo(star_date, end_year)
    credenciales_datalake_economico = load_sipa_valores(credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database).conectar_bdd()
    credenciales_datalake_economico.read_sipa(star_date, end_year)