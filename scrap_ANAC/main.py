from home_page import HomePage
from anac_armadoDF import armadoDF
import sys
import os
import pandas as pd
from loadDatabase import load_database
from save_data_sheet import readSheets

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


if __name__ == "__main__":
    #home_page = HomePage()
    #home_page.descargar_archivo()

    directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
    file_path_desagregado = os.path.join(ruta_carpeta_files, 'anac.xlsx')

    df = armadoDF.armadoDF(file_path_desagregado)
    
    credenciales_datalake_economico = load_database(credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database).conectar_bdd()
    credenciales_datalake_economico.load_data(df)
    values = credenciales_datalake_economico.read_data_excel()
    print (values)
    readSheets().escribir_fila(values)