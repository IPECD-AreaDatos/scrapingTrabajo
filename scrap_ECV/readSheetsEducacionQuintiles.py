from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd
import numpy as np
from datetime import datetime

class readSheetsEducacionQuintiles:   
    def leer_datos_educacionQuintiles(self):
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

        #Realiza una llamada a la API para obtener datos desde la hoja 'Hoja 1' en el rango 'A1:A8'
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Educacion_Q!A:P').execute()
        # Extrae los valores del resultado
        values = result.get('values', [])[1:]
        
        # Crea el DataFrame df1
        df = pd.DataFrame(values, columns=['Aglomerado', 'Año', 'Fecha', 'Trimestre', 'Estado del dato' , 'Quintil', 'Primaria incompleta', 'Primaria completa', 'Secundaria incompleta', 'Secundaria completa', 'Superior incompleto', 'Superior completo', 'Sin instrucción', 'Asisencia escolar', 'Institución Pública', 'Institución Privada'])
        df = df.dropna(subset=['Quintil']) #<--------ELIMINA LAS FILAS QUE NO TIENEN QUINTIL
        print(df)
        for e in df['Estado del dato']:
            if e != 'FINALIZADO':
                e=' '
               #df.replace({e:pd.NA}, inplace=True)
        df.replace({" ": pd.NA, "": pd.NA}, inplace=True)
        df.dropna(subset=['Estado del dato'], inplace=True)    
        df = df.where(pd.notnull(df), None)
        print(df)
        #print(df.iloc[:,:6])
        df.drop(['Estado del dato'], axis=1, inplace=True)
        print(df.dtypes)
        self.transformar_tipo_datos(df)

        print(df)
        print(df.columns)
        return df
        

    def transformar_tipo_datos(self, df):
        # Seleccionar las columnas numéricas
        columnas_numericas = ['Primaria incompleta', 'Primaria completa', 'Secundaria incompleta', 'Secundaria completa', 'Superior incompleto', 'Superior completo', 'Sin instrucción', 'Asisencia escolar', 'Institución Pública', 'Institución Privada']
        # Convertir las columnas numéricas a tipos numéricos
        # Elimina el símbolo "%" y las comas, y luego convierte las columnas en valores numéricos
        for columna in columnas_numericas:
            # Crea una serie booleana que indica si la celda no es nula
            no_nulos = df[columna].notnull()
            # Aplica la conversión solo a las celdas no nulas
            df.loc[no_nulos, columna] = df.loc[no_nulos, columna].replace({'%': '', ',': '.'}, regex=True).apply(pd.to_numeric)
            df.loc[no_nulos, columna] = df.loc[no_nulos, columna] / 100
        # Convertir la primera columna a tipo de datos de fecha
        df['Año'] = df['Año'].astype(int)
        df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y')
        df['Fecha'] = df['Fecha'].dt.strftime('%Y-%m-%d')  # Formatear a 'YYYY-MM-DD'
        # Convertir la segunda columna a tipo de datos entero
        df['Trimestre'] = df['Trimestre'].astype(str)
        df['Aglomerado'] = df['Aglomerado'].astype(str)
        df['Quintil'] = df['Quintil'].astype(int)

