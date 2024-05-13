from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd



class readSheets:

    def cargar_datos(self,df):

        #Creacion de listas
        autos = list(df['cantidad'][(
                df['id_provincia_indec'] == 18) 
                & (df['id_vehiculo'] == 1) 
                & (df['fecha'] >= '2018-12-01')])
        

        
        motos = df['cantidad'][(
                df['id_provincia_indec'] == 18) 
                & (df['id_vehiculo'] == 2) 
                & (df['fecha'] >= '2018-12-01')]
        

        print(autos)


        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        
        #Direccion del archivo json 
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        KEY = os.path.join(directorio_desagregado, 'key.json')

        #ID del documento:
        SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'

        # Carga las credenciales desde el archivo JSON
        creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)

        # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Borra los datos de la fila desde la columna C7 hasta el final de la fila
        #request = sheet.values().clear(spreadsheetId=SPREADSHEET_ID,
        #                                range='Datos!C7:7',  # Desde la columna C7 hasta el final de la fila
        #                                body={})
        
        # Borra los datos de la fila desde la columna C7 hasta el final de la fila
        request = sheet.values().append(spreadsheetId=SPREADSHEET_ID,
                                        range='Datos!C7:7',  # Desde la columna C7 hasta el final de la fila
                                        body={'values':autos})      

        response = request.execute()
            

import pymysql
import pandas as pd

host = '54.94.131.196'
user = 'estadistica'
password = 'Estadistica2024!!'
database = 'datalake_economico'

# Conexión a la base de datos
conn = pymysql.connect(host=host, user=user, password=password, database=database)

# Consulta SQL para obtener fechas con id_tipo_registro = 8
query_8 = "SELECT * FROM dnrpa"
df_8 = pd.read_sql(query_8, conn)
df_8['fecha'] = pd.to_datetime(df_8['fecha'])

readSheets().cargar_datos(df_8)
