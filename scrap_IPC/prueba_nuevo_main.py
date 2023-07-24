from homePage import HomePage
from prueba_cambio_ipc import LoadXLSDataCuyo
import os
import pandas as pd

"""
--- PASOS ---

1) Construir EXCEl
2) Verificar fechas para actualizacion

"""
#Listas a tratar durante el proceso
lista_fechas = list()
lista_regiones = list()
lista_subdivision = list()
lista_valores = list()

df = pd.DataFrame()

#Descargar EXCEL - Tambien almacenamos las rutas que usaremos
url = HomePage()
directorio_actual = os.path.dirname(os.path.abspath(__file__))
ruta_carpeta_files = os.path.join(directorio_actual, 'files')
file_path = os.path.join(ruta_carpeta_files, 'archivo.xls')


LoadXLSDataCuyo().loadInDataBase(file_path, lista_fechas ,lista_regiones,lista_subdivision, lista_valores)

df['fecha'] = lista_fechas
df['regiones'] = lista_regiones
df['subdivision'] = lista_subdivision
df['valores'] = lista_valores


for i in df.values:

    print(i)