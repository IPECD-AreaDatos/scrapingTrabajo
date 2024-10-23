from readData import readData
from readHistory import ripte_cargaHistorico
from loadDataBaseRIPTE import ripte_cargaUltimoDato
import sys
import os
# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))


if __name__ == "__main__":
    # Descarga del archivo
    #readData().descargar_archivo()
    #ripte_cargaHistorico().loadInDataBase(credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database)
    
    # Scrip principal
    # Obtenemos el ultimo valor de RIPTE desde la pagina de inicio
    ultimo_valor_ripte = readData().extract_last_value()
    
    # Carga del último dato en la base de datos Datalake Economico
    conexion = ripte_cargaUltimoDato(
        host_dbb,user_dbb,pass_dbb,dbb_datalake
    )
    conexion.loadInDataBaseDatalakeEconomico(ultimo_valor_ripte)

    