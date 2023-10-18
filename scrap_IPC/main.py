from homePage import HomePage
from armadoXLSdataGBA import LoadXLSDataGBA
from armadoXLSDataPampeana import LoadXLSDataPampeana
from armadoXLSDataNOA import LoadXLSDataNOA
from armadoXLSDataNEA import LoadXLSDataNEA
from armadoXLSDatacuyo import LoadXLSDataCuyo
from armadoXLSDataPatagonia import LoadXLSDataPatagonia
from conexionBaseDatos import conexionBaseDatos
from armadoXLSProductos import LoadXLSDataProductos
import os


#Listas a tratar durante el proceso
lista_fechas = list()
lista_region = list()
lista_categoria = list()
lista_division = list()
lista_subdivision = list()
lista_valores = list()

#Datos de la base de datos
host = '172.17.16.157'
user = 'team-datos'
password = 'HCj_BmbCtTuCv5}'
database = 'ipecd_economico'

valor_region = 2

if __name__ == '__main__':
    #Descargar EXCEL - Tambien almacenamos las rutas que usaremos
    home_page = HomePage()
    home_page.descargar_archivo()
    
    #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ CARGA DE IPC DESAGREGADO ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
    directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
    file_path_desagregado = os.path.join(ruta_carpeta_files, 'IPC_Desagregado.xls')
    valoresDeIPC = [
      LoadXLSDataGBA,
      LoadXLSDataPampeana,
      LoadXLSDataNOA,
      LoadXLSDataNEA,
      LoadXLSDataCuyo,
      LoadXLSDataPatagonia,
    ]
    for regiones in valoresDeIPC:
      print("Valor region: ", valor_region)
      regiones().loadInDataBase(file_path_desagregado, valor_region, lista_fechas, lista_region,  lista_categoria, lista_division, lista_subdivision, lista_valores)
      valor_region = valor_region + 1
    
    instancia = conexionBaseDatos(lista_fechas, lista_region, lista_categoria, lista_division, lista_subdivision, lista_valores, host, user, password, database)
    instancia.cargaBaseDatos()
    
    #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ CARGA DE IPC PRODUCTOS ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
    directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
    file_path_productos = os.path.join(ruta_carpeta_files, 'IPC_Productos.xls')
    LoadXLSDataProductos().loadInDataBase(file_path_productos)



