from extract import HomePage
import os
import pandas as pd
#from homePage import HomePage
from cargaIndice import cargaIndice

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
credenciales_datalakeEconomico = Credenciales('datalake_economico')


if __name__ == '__main__':
    
    #Obtencion del archivo
    #url = HomePage().descargar_archivos()

    # Obtener la ruta del directorio actual (donde se encuentra el script)
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')

    # Construir las rutas de los archivos
    file_path = os.path.join(ruta_carpeta_files, 'EMAE.xls')
    file_path_variacion = os.path.join(ruta_carpeta_files, 'EMAEVAR.xls')
 
    #Inicializamos el data frame y la listas de datos
    df = pd.DataFrame() 
    lista_fechas= list()
    lista_SectorProductivo = list()
    lista_valores= list() 

    cargaIndice().loadXLSIndiceEMAE(file_path, lista_fechas, lista_SectorProductivo, lista_valores, credenciales_datalakeEconomico.host, credenciales_datalakeEconomico.user, credenciales_datalakeEconomico.password, credenciales_datalakeEconomico.database)
    #cargaIndice().loadXLSVariacionEMAE(file_path_variacion, lista_fechas, credenciales_datalakeEconomico.host, credenciales_datalakeEconomico.user, credenciales_datalakeEconomico.password, credenciales_datalakeEconomico.database)
    
