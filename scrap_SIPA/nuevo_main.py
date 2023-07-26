from homePage import HomePage
from loadXLSProvincias import LoadXLSProvincias
from loadXLSTrabajoRegistrado import LoadXLSTrabajoRegistrado
from calculoTotalNacion import calculoTotalNacion
import os
import pandas as pd


#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'



#Obtencion del archivo
url = HomePage()
directorio_actual = os.path.dirname(os.path.abspath(__file__))
ruta_carpeta_files = os.path.join(directorio_actual, 'files')
file_path = os.path.join(ruta_carpeta_files, 'SIPA.xlsx')




"""
========================================================================================

                    CARGA DE PROVINCIAS

========================================================================================

"""



df = pd.DataFrame() #--> Contendra todos los datos 
lista_provincias = list() #--> Contendra los indices de la provincia
lista_valores_estacionalidad = list() #--> Contendra los valores estacionales
lista_valores_sin_estacionalidad = list() #--> Contendra los valores no estacionales
lista_registro = list()#--> Contendra el tipo de REGISTRO
lista_fechas= list() #--> Contendra las fechas


LoadXLSProvincias().loadInDataBase(file_path, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas)
LoadXLSTrabajoRegistrado().loadInDataBase(file_path, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas)



df['fecha'] = lista_fechas
df['id_prov'] = lista_provincias
df['tipo_registro'] = lista_registro
df['valores_estacionales'] = lista_valores_estacionalidad
df['valores_no_estacionales'] = lista_valores_sin_estacionalidad

#Ordenamiento por fecha
df = df.sort_values('fecha')

for i in df['tipo_registro']:

    print(i)



