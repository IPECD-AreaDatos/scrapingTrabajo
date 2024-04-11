from extract import HomePage
from ripte_cargaHistorico import ripte_cargaHistorico
from ripte_cargaUltimoDato import ripte_cargaUltimoDato
import sys
import os


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
credenciales_datalake_economico = Credenciales("datalake_economico")
credenciales_ipecd_economico = Credenciales("ipecd_economico")

# Instancia encargada

if __name__ == "__main__":
    # Obtencion del archivo
    home_page = HomePage()
    home_page.descargar_archivo()
    # ripte_cargaHistorico().loadInDataBase(credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database)
    # ↓↓↓↓↓↓↓↓↓↓↓↓CARGA DEL TABLERO ↓↓↓↓↓↓↓↓↓↓↓↓
    # ripte_cargaHistorico().loadInDataBase(credenciales_ipecd_economico.host, credenciales_ipecd_economico.user, credenciales_ipecd_economico.password, credenciales_ipecd_economico.database)

    # Scrip principal
    # Obtenemos el ultimo valor de RIPTE desde la pagina de inicio
    ultimo_valor_ripte = HomePage().extract_last_date()
    print(ultimo_valor_ripte)
    # Carga del último dato en la base de datos Datalake Economico
    instancia = ripte_cargaUltimoDato(
        credenciales_datalake_economico.host,
        credenciales_datalake_economico.user,
        credenciales_datalake_economico.password,
        credenciales_datalake_economico.database,
    )
    instancia.loadInDataBaseDatalakeEconomico()

    # ↓↓↓↓↓↓↓↓↓↓↓↓CARGA DEL TABLERO ↓↓↓↓↓↓↓↓↓↓↓↓
    # Carga del último dato en la base de datos IPECD Economico
    instancia = ripte_cargaUltimoDato(
        credenciales_ipecd_economico.host,
        credenciales_ipecd_economico.user,
        credenciales_ipecd_economico.password,
        credenciales_ipecd_economico.database,
    )
    instancia.loadInDataBaseIPECD_Economico()
