from extract import HomePage
from construccion_listas import ExtractData
from conexionBaseDatos import conexionBaseDatos
from armadoInformePDF import googleSheets
import os
import pandas as pd
import sys
from send_mail_sipa import MailSipa

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)


from credenciales_bdd import Credenciales


credenciales = Credenciales("datalake_economico")


if __name__ == '__main__':
 
    
    #url = HomePage()
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')
    file_path = os.path.join(ruta_carpeta_files, 'SIPA.xlsx')

    #ZONA DE EXTRACCION DE DATOS
    #Creamos las variables. Las listas las usamos para luego añadirlas a un dataframe
    lista_provincias = list()
    lista_valores_estacionalidad = list() 
    lista_valores_sin_estacionalidad = list() 
    lista_registro = list()
    lista_fechas= list() 

    #Buscamos los datos por provincia, y hacemos una busqueda aparte para los datos de la nacion
    instancia_listas= ExtractData()
    instancia_listas.listado_provincias(file_path, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas)
    instancia_listas.listado_nacion(file_path, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas)

    #Asignamos las listas al dataframe
    df = pd.DataFrame() 
    df['fecha'] = lista_fechas
    df['id_provincia'] = lista_provincias
    df['id_tipo_registro'] = lista_registro
    df['cantidad_con_estacionalidad'] = lista_valores_estacionalidad
    df['cantidad_sin_estacionalidad'] = lista_valores_sin_estacionalidad

    print(df)
    instancia_bdd = conexionBaseDatos(credenciales.host, credenciales.user, credenciales.password, credenciales.database)

    bandera_correo = instancia_bdd.load_datalake(df)


    if bandera_correo:

        #Instancia de correo
        instancia_correo = MailSipa(credenciales.host, credenciales.user, credenciales.password, "dwh_economico")
        instancia_correo.connect_db()
        instancia_correo.send_mail()


