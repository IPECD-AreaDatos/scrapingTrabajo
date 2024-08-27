from readData import readData
from readHistory import ripte_cargaHistorico
from loadDataBaseRIPTE import ripte_cargaUltimoDato
import sys
import os

# Obtener la ruta al directorio que contiene las Credenciales
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, "..", "Credenciales_folder")
sys.path.append(credenciales_dir)
from credenciales_bdd import Credenciales

# Credenciales de las bases de datos a utilizar
credenciales_datalake_economico = Credenciales("datalake_economico")
credenciales_ipecd_economico = Credenciales("ipecd_economico")


if __name__ == "__main__":
    # Descarga del archivo
    #readData().descargar_archivo()
    #ripte_cargaHistorico().loadInDataBase(credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database)
    
    # Scrip principal
    # Obtenemos el ultimo valor de RIPTE desde la pagina de inicio
    ultimo_valor_ripte = readData().extract_last_value()
    
    # Carga del último dato en la base de datos Datalake Economico
    conexion = ripte_cargaUltimoDato(
        credenciales_datalake_economico.host,
        credenciales_datalake_economico.user,
        credenciales_datalake_economico.password,
        credenciales_datalake_economico.database,
    )
    conexion.loadInDataBaseDatalakeEconomico(ultimo_valor_ripte)

    