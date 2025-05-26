from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from json import loads
from google.oauth2 import service_account
import os
import pandas as pd


class readSheets:
    def escribir_fila(self, values):
        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        
        #CARGAMOS LA KEY DE LA API y la convertimos a un JSON, ya que se almacena como str
        key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))

        #ID del documento:
        SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'

        # Carga las credenciales desde el archivo JSON
        creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)

        # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()
        
        # Asegúrate de que `values` sea una lista de listas
        body = {'values': [values]}
        # Actualizar la hoja de cálculo
        request = sheet.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f'Datos!D10:10',
            valueInputOption='RAW',
            body=body
        ).execute()
        
        print(f"{request.get('updatedCells')} celdas actualizadas en el sheets!")