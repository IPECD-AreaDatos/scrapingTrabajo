# Agrega la ruta del folder que contiene los módulos al sistema de rutas de Python
from armadoXLSdataGBA import LoadXLSDataGBA
from armadoXLSDataPampeana import LoadXLSDataPampeana
from armadoXLSDataNOA import LoadXLSDataNOA
from armadoXLSDataNEA import LoadXLSDataNEA
from armadoXLSDatacuyo import LoadXLSDataCuyo
from armadoXLSDataPatagonia import LoadXLSDataPatagonia
from conexionBaseDatos import conexionBaseDatos
from armadoXLSProductos import LoadXLSDataProductos
import os
import sys
from homePage import HomePage
import pandas as pd

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
instancia_credenciales = Credenciales('datalake_economico')


print("Las credenciales son", instancia_credenciales.host,instancia_credenciales.user,instancia_credenciales.password,instancia_credenciales.database)

valor_region = 2

if __name__ == '__main__':
    
    #Listas a tratar durante el proceso
    lista_fechas = list()
    lista_region = list()
    lista_categoria = list()
    lista_division = list()
    lista_subdivision = list()
    lista_valores = list()

    #Descargar EXCEL - Tambien almacenamos las rutas que usaremos
    home_page = HomePage()
    home_page.descargar_archivo()
    
    #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ CARGA DE IPC DESAGREGADO ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
    directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
    file_path_desagregado = os.path.join(ruta_carpeta_files, 'IPC_Desagregado.xls')
    valoresDeIPC = [
      LoadXLSDataGBA,

    ]
    for regiones in valoresDeIPC:
      print("Valor region: ", valor_region)
      regiones().loadInDataBase(file_path_desagregado, valor_region, lista_fechas, lista_region,  lista_categoria, lista_division, lista_subdivision, lista_valores)
      valor_region = valor_region + 1


    #Creamos el DF que contendra los datos
    df_variaciones = pd.DataFrame()
    df_variaciones['fecha'] = lista_fechas
    df_variaciones['id_region'] = lista_region
    df_variaciones['id_categoria'] = lista_categoria
    df_variaciones['id_division'] = lista_division
    df_variaciones['id_subdivision'] = lista_subdivision
    df_variaciones['variaciones'] = lista_valores
