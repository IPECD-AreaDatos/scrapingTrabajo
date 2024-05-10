from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd



class readSheets:

    def cargar_datos(self,df):

        #Creacion de listas
        autos = df['cantidad'][(
                df['id_provincia_indec'] == 18) 
                & (df['id_vehiculo'] == 1) 
                & (df['fecha'] >= '2018-12-01')]
        
        motos = df['cantidad'][(
                df['id_provincia_indec'] == 18) 
                & (df['id_vehiculo'] == 2) 
                & (df['fecha'] >= '2018-12-01')]
        
        print(autos)
        print(motos)


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

        # Obtén los datos de la hoja de cálculo
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Datos!7:7').execute()
        values = result.get('values', [])

        # Imprime los datos
        if not values:
            print('No se encontraron datos.')
        else:
            print('Contenido de la hoja de cálculo:')
            for row in values:
                print('\t'.join(row))


