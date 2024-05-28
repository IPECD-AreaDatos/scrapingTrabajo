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
        
        # Eliminar todos los ceros
        autos = [dato for dato in autos if dato != 0]
        lista_de_lista_autos = [autos]

        motos = df['cantidad'][(
                df['id_provincia_indec'] == 18) 
                & (df['id_vehiculo'] == 2) 
                & (df['fecha'] >= '2018-12-01')]

        motos = [dato for dato in motos if dato != 0]
        lista_de_lista_motos = [motos]


        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        
        #Direccion del archivo json 
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        KEY = os.path.join(ruta_carpeta_files, 'key.json')

        #ID del documento:
        SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'

        # Carga las credenciales desde el archivo JSON
        creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)

        # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()


        #Remplzamos los datos en la fila correspondiente
        request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,
                                        range='Datos!C7:7',
                                        valueInputOption='RAW',
                                        body={'values':lista_de_lista_autos}).execute()
        
        #Remplzamos los datos en la fila correspondiente
        request = sheet.values().update(spreadsheetId=SPREADSHEET_ID,
                                        range='Datos!C8:8',
                                        valueInputOption='RAW',
                                        body={'values':lista_de_lista_motos}).execute()
        

