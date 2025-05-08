from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd
from json import loads
import json

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv

"""
Este script saca los datos de la hoja de sheets: https://docs.google.com/spreadsheets/d/1sfAdpqs9oh6JbP5kZgiirHAx99tn7ELxz7TZWIe3BrM/edit?gid=0#gid=0
De la hoja EPH
"""

class readSheetsPobrezaEPH:
    def leer_datos_tasas(self):
        load_dotenv()
        df = []
        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))

        creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)

        # Escribe aquí el ID de tu documento:
        SPREADSHEET_ID = '1sfAdpqs9oh6JbP5kZgiirHAx99tn7ELxz7TZWIe3BrM'

        # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Realiza una llamada a la API para obtener datos desde la hoja 'Hoja 1' en el rango 'A1:A8'
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Pobreza_EPH!A:K').execute()
        # Extrae los valores del resultado
        values = result.get('values', [])[1:]

        # Crea el DataFrame df1
        df = pd.DataFrame(values, columns=['region', 'aglomerado', 'año', 'fecha', 'semestre', 'pobreza', 'indigencia', 'varsem_pobreza', 'varsem_indigencia', 'varanual_pobreza', 'varanual_indigencia'])
        df.replace({" ": pd.NA, "": pd.NA}, inplace=True)
        df = df.where(pd.notnull(df), None)
        print(df)
        self.transformar_tipo_datos(df)
        print(df.dtypes)
        print(df)
        print(df.columns)
        return df
        

    def transformar_tipo_datos(self, df):
        # Seleccionar las columnas numéricas
        columnas_numericas = ['pobreza', 'indigencia', 'varsem_pobreza', 'varsem_indigencia', 'varanual_pobreza', 'varanual_indigencia']
        # Convertir las columnas numéricas a tipos numéricos
        # Eliminar el símbolo "%" y las comas, y luego convertir las columnas en valores numéricos
        df[columnas_numericas] = df[columnas_numericas].replace({'%': '', ',': '.'}, regex=True).apply(pd.to_numeric)

        # Dividir los valores numéricos por 100
        df[columnas_numericas] = df[columnas_numericas] / 100
        # Convertir la segunda columna a tipo de datos entero
        df['año'] = df['año'].astype(int)
        df['fecha'] = pd.to_datetime(df['fecha'])
