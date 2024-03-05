from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd


class readSheets:
    def leer_datos_tasas(self):
        df = []
        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        KEY = os.path.join(ruta_carpeta_files, 'key.json')

        # Escribe aquí el ID de tu documento:
        SPREADSHEET_ID = '1sfAdpqs9oh6JbP5kZgiirHAx99tn7ELxz7TZWIe3BrM'

        # Carga las credenciales desde el archivo JSON
        creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)

        # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Realiza una llamada a la API para obtener datos desde la hoja 'Hoja 1' en el rango 'A1:A8'
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Trabajo_EPH!A:G').execute()
        # Extrae los valores del resultado
        values = result.get('values', [])[1:]

        # Crea el DataFrame df1
        df = pd.DataFrame(values, columns=['Aglomerado', 'Año', 'Fecha', 'Trimestre', 'Tasa de Actividad', 'Tasa de Empleo', 'Tasa de desocupación'])
        df = df.where(pd.notnull(df), None)
        self.transformar_tipo_datos(df)
        print(df.dtypes)
        print(df)
        print(df.columns)
        return df
        

    def transformar_tipo_datos(self, df):
        # Seleccionar las columnas numéricas
        columnas_numericas = ['Tasa de Actividad', 'Tasa de Empleo', 'Tasa de desocupación']
        # Convertir las columnas numéricas a tipos numéricos
        # Eliminar el símbolo "%" y las comas, y luego convertir las columnas en valores numéricos
        df[columnas_numericas] = df[columnas_numericas].replace({'%': '', ',': '.'}, regex=True).apply(pd.to_numeric)

        # Dividir los valores numéricos por 100
        df[columnas_numericas] = df[columnas_numericas] / 100
        # Convertir la primera columna a tipo de datos de fecha
        df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y')
        # Convertir la segunda columna a tipo de datos entero
        df['Trimestre'] = df['Trimestre'].astype(str)
        df['Aglomerado'] = df['Aglomerado'].astype(str)
        df['Año'] = df['Año'].astype(int)
        
