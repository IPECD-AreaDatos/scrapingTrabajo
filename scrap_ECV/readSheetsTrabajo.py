from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from json import loads


class readSheetsTrabajo:   
    def leer_datos_trabajo(self):
        df = []

        load_dotenv()
        
        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

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
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Trabajo_ECV!A:U').execute()
        # Extrae los valores del resultado
        values = result.get('values', [])[1:]
        
        # Crea el DataFrame df1
        df = pd.DataFrame(values, columns=['Aglomerado', 'Año', 'Fecha', 'Trimestre','Estado del dato', 'Tasa de Actividad', 'Tasa de Empleo', 'Tasa de desocupación', 'Trabajo Público', 'Trabajo Privado', 'Trabajo Otro', 'Trabajo Privado Registrado', 'Trabajo Privado No Registrado', 'Salario Promedio Público', 'Salario Promedio Privado', 'Salario Promedio Privado Registrado', 'Salario Promedio Privado No Registrado', 'Patron', 'Cuenta Propia', 'Empleado/Obrero', 'Trabajador familiar sin remuneración'])
        print(df)
        for e in df['Estado del dato']:
            if e != 'FINALIZADO':
                e=' '
        df.replace({" ": pd.NA, "": pd.NA}, inplace=True)
        df.dropna(subset=['Estado del dato'], inplace=True)    
        df = df.where(pd.notnull(df), None)
        print(df)
        #print(df.iloc[:,:6])
        df.drop(['Estado del dato'], axis=1, inplace=True)

        print(df.dtypes)
        self.transformar_tipo_datos(df)
        df = df[['Aglomerado', 'Año', 'Fecha', 'Trimestre', 
                'Tasa de Actividad', 'Tasa de Empleo', 'Tasa de desocupación', 
                'Trabajo Privado', 'Trabajo Público', 'Trabajo Otro', 
                'Trabajo Privado Registrado', 'Trabajo Privado No Registrado', 
                'Salario Promedio Público', 'Salario Promedio Privado', 
                'Salario Promedio Privado Registrado', 'Salario Promedio Privado No Registrado', 
                'Patron', 'Cuenta Propia', 'Empleado/Obrero', 
                'Trabajador familiar sin remuneración']]

        print(df)
        print(df.columns)
        return df
        

    def transformar_tipo_datos(self, df):
        # Seleccionar las columnas numéricas
        columnas_numericas_porcentajes = ['Tasa de Actividad', 'Tasa de Empleo', 'Tasa de desocupación', 'Trabajo Privado', 'Trabajo Público', 'Trabajo Otro', 'Trabajo Privado Registrado', 'Trabajo Privado No Registrado',  'Patron', 'Cuenta Propia', 'Empleado/Obrero', 'Trabajador familiar sin remuneración']
        df[columnas_numericas_porcentajes] = df[columnas_numericas_porcentajes].replace({'%': '', ',': '.'}, regex=True).apply(pd.to_numeric)
        # Divide los valores numéricos por 100
        df[columnas_numericas_porcentajes] = df[columnas_numericas_porcentajes] / 100
        
        columnas_numericas=['Salario Promedio Público', 'Salario Promedio Privado', 'Salario Promedio Privado Registrado', 'Salario Promedio Privado No Registrado']
        df[columnas_numericas] = df[columnas_numericas].replace({' ': '', '\$': '', ',': ''}, regex=True).apply(pd.to_numeric)

        # Convertir la primera columna a tipo de datos de fecha
        df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y')

        df['Año'] = df['Año'].astype(int)
        # Convertir la segunda columna a tipo de datos entero
        df['Trimestre'] = df['Trimestre'].astype(str)
        df['Aglomerado'] = df['Aglomerado'].astype(str)

