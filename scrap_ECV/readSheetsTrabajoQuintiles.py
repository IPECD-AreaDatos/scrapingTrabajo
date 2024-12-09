from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from json import loads

class readSheetsTrabajoQuintiles:   
    def leer_datos_trabajo_quintiles(self):
        df = []
        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        load_dotenv()
        # CARGAMOS LA KEY DE LA API y la convertimos a un JSON, ya que se almacena como str
        key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))

        # Carga las credenciales desde el diccionario JSON
        creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)


        # Escribe aquí el ID de tu documento:
        SPREADSHEET_ID = '1sfAdpqs9oh6JbP5kZgiirHAx99tn7ELxz7TZWIe3BrM'

        # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        #Realiza una llamada a la API para obtener datos desde la hoja 'Hoja 1' en el rango 'A1:A8'
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Trabajo_Q!A:T').execute()
        # Extrae los valores del resultado
        values = result.get('values', [])[1:] #<--------CAMBIAR CUANDO JOSE ACTUALICE LA TABLA
        
        # Crea el DataFrame df1
        df = pd.DataFrame(values, columns=['Aglomerado', 'Año', 'Fecha', 'Trimestre','Estado del dato', 'Quintil', 'Empleo Público', 'Empleo Privado', 'Empleo Otro', 'Patron', 'Cuenta Propia', 'Obrero o Empleado', 'Trabajador Familiar sin Remuneracion', 'Primaria Incompleta', 'Primaria Completa', 'Secundaria Incompleta', 'Secundaria Completa', 'Superior o Universitario Incompleto', 'Superior o Universitario Completo', 'Sin Instruccion'])
        print(df)
        for e in df['Estado del dato']:
            if e != 'FINALIZADO':
                e=' '
        df.replace({" ": pd.NA, "": pd.NA}, inplace=True)
        df.dropna(subset=['Estado del dato'], inplace=True)    
        df = df.where(pd.notnull(df), None)
        print(df)
        #print(df.iloc[:,:7])
        df.drop(['Estado del dato'], axis=1, inplace=True)
        print(df.dtypes)
        self.transformar_tipo_datos(df)

        print(df)
        print(df.columns)
        return df
        

    def transformar_tipo_datos(self, df):
        # Seleccionar las columnas numéricas
        columnas_numericas_porcentajes = ['Empleo Público', 'Empleo Privado', 'Empleo Otro', 'Patron', 'Cuenta Propia', 'Obrero o Empleado', 'Trabajador Familiar sin Remuneracion', 'Primaria Incompleta', 'Primaria Completa', 'Secundaria Incompleta', 'Secundaria Completa', 'Superior o Universitario Incompleto', 'Superior o Universitario Completo', 'Sin Instruccion']
        df[columnas_numericas_porcentajes] = df[columnas_numericas_porcentajes].replace({'%': '', ',': '.'}, regex=True).apply(pd.to_numeric)
        # Divide los valores numéricos por 100
        df[columnas_numericas_porcentajes] = df[columnas_numericas_porcentajes] / 100

        df['Quintil'] = df['Quintil'].astype(int)
        # Convertir la primera columna a tipo de datos de fecha
        df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y')
        df['Fecha'] = df['Fecha'].dt.strftime('%Y-%m-%d')  # Formatear a 'YYYY-MM-DD'

        df['Año'] = df['Año'].astype(int)
        # Convertir la segunda columna a tipo de datos entero
        df['Trimestre'] = df['Trimestre'].astype(str)
        df['Aglomerado'] = df['Aglomerado'].astype(str)

