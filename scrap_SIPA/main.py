from extract import HomePage
from construccion_listas import ExtractData
from conexionBaseDatos import conexionBaseDatos
from armadoInformePDF import googleSheets
import os
import pandas as pd
import sys
from send_mail_sipa import MailSipa
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))


if __name__ == '__main__':    
    url = HomePage()
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
    df = df.sort_values(by = ['fecha','id_provincia','id_tipo_registro'])

    print(df)
    instancia_bdd = conexionBaseDatos(host_dbb, user_dbb,pass_dbb, dbb_datalake)
    print("xdddddddddddddddddddddddd")
    bandera_correo = instancia_bdd.load_datalake(df)
    print("noooooooo")


    if bandera_correo:
        print("siiiiiiiiiii")

        #Instancia de correo
        instancia_correo = MailSipa(host_dbb, user_dbb,pass_dbb, dbb_datalake)
        instancia_correo.connect_db()
        instancia_correo.send_mail()
        print("Correo enviado!")

