from homePage_CBT import HomePageCBT
from homePage_Pobreza import HomePagePobreza
from Transform_CbtCba import loadXLSDataCBT
from connectionDataBase import connection_db
from send_mail import MailCBTCBA
import os
import sys 
import requests
import pandas as pd

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_SOCIO'))
dbb_dwh = (os.getenv('NAME_DBB_DWH_SOCIO'))

if __name__ == '__main__':
    # ZONA DE EXTRACT -- Donde se buscan los datos
    # Descargar archivos de HomePageCBT y HomePagePobreza
    home_page_CBT = HomePageCBT()
    home_page_CBT.descargar_archivo()

    home_page_Pobreza = HomePagePobreza()
    home_page_Pobreza.descargar_archivo()

    # Transformar datos del archivo Excel de HomePageCBT
    df = loadXLSDataCBT().transform_datalake()
    print(df)

    # Conexión y carga de datos en la base de datos del DataLake Sociodemográfico
    instancia_conexion_bdd = connection_db(host_dbb,user_dbb,pass_dbb,dbb_datalake)
    instancia_conexion_bdd.connect_db()
    bandera_correo = instancia_conexion_bdd.load_datalake(df)
    
    # Si se cargaron los datos correctamente en el DataLake Sociodemográfico, enviar correo.
    if bandera_correo:
        instancia_correo = MailCBTCBA(host_dbb,user_dbb,pass_dbb,dbb_dwh)
        instancia_correo.send_mail_cbt_cba()
        
        # Pegada a la API
        # Filtrar las últimas filas
        last_row = df.tail(1).iloc[0]  # Esto obtiene la última fila del DataFrame

        # Extraer los valores necesarios
        anio = last_row['Fecha'].year  # Año de la columna 'Fecha'
        mes = last_row['Fecha'].month  # Mes de la columna 'Fecha'
        cbt = last_row['cbt_nea']  # Valor de 'cbt_nea'
        cba = last_row['cba_nea']  # Valor de 'cba_nea'

        # Endpoint
        url = "https://ecv.corrientes.gob.ar/api/create_cbt"

        # Datos que se van a enviar en la solicitud POST
        data = {
            "anio": anio,
            "mes": mes,
            "cbt": int(cbt),  # Convertimos a entero si es necesario
            "cba": int(cba)   # Convertimos a entero si es necesario
        }

        # Realizar la solicitud POST
        response = requests.post(url, data=data)

        # Verificar la respuesta
        if response.status_code == 200:
            print("Solicitud exitosa:", response.json())
        else:
            print(f"Error en la solicitud: {response.status_code}")
            print(response.text)
        

