from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd

class readSheetsTransporteDesplazamiento:   
    def leer_datos_trabajo(self):
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
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Transporte_Desplazamiento_ECV!A:U').execute()
        # Extrae los valores del resultado
        values = result.get('values', [])[1:]
        
        # Crea el DataFrame df1
        df = pd.DataFrame(values, columns=['aglomerado', 'año', 'trimestre','fecha','estado_dato', 'no_desplaza', 'desplaza_en_municipio', 'desplaza_fuera_municipio', 'sin_desplazamiento_fijo'])
        print(df)

        for i, e in enumerate(df['estado_dato']):
            if e != 'FINALIZADO':
                df.loc[i, 'estado_dato'] = None

        
        df.dropna(subset=['estado_dato'], inplace=True)
        df.replace({'': pd.NA, ' ': pd.NA}, inplace=True)

          
        df = df.where(pd.notnull(df), None)
        print(df)
        #print(df.iloc[:,:6])
        df.drop(['estado_dato'], axis=1, inplace=True)

        print(df.dtypes)
        self.transformar_tipo_datos(df)

        print(df)
        print(df.columns)
        return df
        

    def transformar_tipo_datos(self, df):
        # Seleccionar las columnas numéricas
        columnas_numericas_porcentajes = ['no_desplaza', 'desplaza_en_municipio', 'desplaza_fuera_municipio', 'sin_desplazamiento_fijo']
        df[columnas_numericas_porcentajes] = df[columnas_numericas_porcentajes].replace({'%': '', ',': '.'}, regex=True).apply(pd.to_numeric)
        # Divide los valores numéricos por 100
        df[columnas_numericas_porcentajes] = df[columnas_numericas_porcentajes] / 100
        
       # columnas_numericas=['Salario Promedio Público', 'Salario Promedio Privado', 'Salario Promedio Privado Registrado', 'Salario Promedio Privado No Registrado']
        #df[columnas_numericas] = df[columnas_numericas].replace({' ': '', '\$': '', ',': ''}, regex=True).apply(pd.to_numeric)

        # Convertir la primera columna a tipo de datos de fecha
        df['fecha'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y')
        df['año'] = df['año'].astype(int)
        # Convertir la segunda columna a tipo de datos entero
        df['trimestre'] = df['trimestre'].astype(str)
        df['aglomerado'] = df['aglomerado'].astype(str)


