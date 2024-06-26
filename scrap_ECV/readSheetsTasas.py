from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd
from datetime import datetime



class readSheetsTasas:
    def leer_datos_tasas(self):
        df = []
        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        KEY = os.path.join(ruta_carpeta_files, 'key.json')

        # Escribe aquí el ID de tu documento:
        SPREADSHEET_ID = '1BHEd_y02Lwjej_2Rkr_HYO7ZU4Y6m2gHEG4uPm0b5Go'

        # Carga las credenciales desde el archivo JSON
        creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)

        # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Realiza una llamada a la API para obtener datos desde la hoja 'Hoja 1' en el rango 'A1:A8'
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='datos trimestrales!A:C').execute()

        # Extrae los valores del resultado
        values = result.get('values', [])[1:]

        # Crea el DataFrame df1
        df1 = pd.DataFrame(values, columns=['Fecha', 'Trimestre', 'Aglomerado'])

        # Realiza una llamada a la API para obtener datos desde la hoja 'Hoja 1' en el rango 'I1:L8'
        result2 = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='datos trimestrales!I:L').execute()

        # Extrae los valores del resultado
        values2 = result2.get('values', [])[1:]

        # Crea el DataFrame df2
        df2 = pd.DataFrame(values2, columns=['Tasa de empleo', 'Tasa de desocupación', 'Tasa de actividad', 'Tasa de inactividad'])

        # Unir los dos DataFrames
        # Realiza una llamada a la API para obtener datos desde la hoja 'Hoja 1' en el rango 'I1:L8'
        result3 = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='datos trimestrales!AY:AY').execute()

        # Extrae los valores del resultado
        values3 = result3.get('values', [])[1:]
        # Crea el DataFrame df2
        df3 = pd.DataFrame(values3, columns=['Estado del dato'])

        df = pd.concat([df1, df2, df3], axis=1)
        # Imprimir el DataFrame resultante
        print(df)
        longitud_primera_fila = len(df.iloc[0])
        # Retornar el DataFrame
        if longitud_primera_fila == 8:
            for e in df['Estado del dato']:
                if e != 'FINALIZADO':
                    e=' '
            df.replace({" ": pd.NA, "": pd.NA}, inplace=True)
            df.dropna(subset=['Estado del dato'], inplace=True)    
            df = df.where(pd.notnull(df), None)
            print(df)
            #print(df.iloc[:,:6])
            df.drop(['Estado del dato'], axis=1, inplace=True)
            print(df)
            self.transformar_tipo_datos(df)
            print(df.dtypes)
            return df
        

    def transformar_tipo_datos(self, df):
        # Seleccionar las columnas numéricas
        columnas_numericas = ['Tasa de empleo', 'Tasa de desocupación', 'Tasa de actividad', 'Tasa de inactividad']
        # Convertir las columnas numéricas a tipos numéricos
        # Elimina el símbolo "%" y las comas, y luego convierte las columnas en valores numéricos
        df[columnas_numericas] = df[columnas_numericas].replace({'%': '', ',': '.'}, regex=True).apply(pd.to_numeric)

        # Divide los valores numéricos por 100
        df[columnas_numericas] = df[columnas_numericas] / 100

        # Convertir la primera columna a tipo de datos de fecha
        df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y')
        df['Fecha'] = df['Fecha'].dt.strftime('%Y-%m-%d')  # Formatear a 'YYYY-MM-DD'

        # Convertir la segunda columna a tipo de datos entero
        df['Trimestre'] = df['Trimestre'].astype(str)
        df['Aglomerado'] = df['Aglomerado'].astype(str)

