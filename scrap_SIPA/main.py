from homePage import HomePage
from loadXLSProvincias import LoadXLSProvincias
from loadXLSTrabajoRegistrado import LoadXLSTrabajoRegistrado
from conexionBaseDatos import conexionBaseDatos
import os
import pandas as pd
import sys

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)


from credenciales_bdd import Credenciales


credenciales = Credenciales()


if __name__ == '__main__':
    
    #Obtencion del archivo
    url = HomePage()
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')
    file_path = os.path.join(ruta_carpeta_files, 'SIPA.xlsx')

    #Inicializamos el data frame y la listas de datos
    df = pd.DataFrame() 
    lista_provincias = list()
    lista_valores_estacionalidad = list() 
    lista_valores_sin_estacionalidad = list() 
    lista_registro = list()
    lista_fechas= list() 

    #Se ejecuta el script de lectura de datos
    LoadXLSProvincias().loadInDataBase(file_path, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas)
    LoadXLSTrabajoRegistrado().loadInDataBase(file_path, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas)

    #Conectar/Cargar/Actualizar la BDD 
    instancia_bdd = conexionBaseDatos(credenciales.host, credenciales.user, credenciales.password, credenciales.database, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas)
    instancia_bdd.cargaBaseDatos()



