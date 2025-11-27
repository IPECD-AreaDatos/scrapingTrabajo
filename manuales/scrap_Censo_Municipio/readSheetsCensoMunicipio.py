from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd
from json import loads

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

class readSheetsCensoMunicipio:
    def leer_datos_censo(self):
        
        df=[]
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        #CARGAMOS LA KEY DE LA API y la convertimos a un JSON, ya que se almacena como str
        key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))

        spreadsheets_ID= '1IBOsYSVDWs9Tz1BN0OlGOZpTsoRNwmiurFJFg__L5ao'

        creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)

        service= build('sheets', 'v4', credentials=creds)

        sheet = service.spreadsheets()

        result = sheet.values().get(spreadsheetId=spreadsheets_ID, range='Datos por municipio!A:F').execute()

        values = result.get('values', [])[7:-3]

        df = pd.DataFrame(values,columns=['municipio','poblacion_viv_part_2010','poblacion_viv_part_2022','var_abs_poblacion_2010_vs_2022', 'peso_relativo_2022','var_rel_poblacion_2010_vs_2022'])

        print(df)
        self.transformar(df)
        print(df)
        print(df.dtypes)

        return df

    def transformar(self, df):
        df['municipio'] = df['municipio'].astype(str)
        print(type(df['municipio'].values[0]))

        # Seleccionar las columnas numéricas
        columnas_numericas = ['poblacion_viv_part_2010','poblacion_viv_part_2022','var_abs_poblacion_2010_vs_2022', 'peso_relativo_2022','var_rel_poblacion_2010_vs_2022']
    
        # Convertir las columnas numéricas a tipos numéricos
        for columna in columnas_numericas:
            # Crea una serie booleana que indica si la celda no es nula
            no_nulos = df[columna].notnull()
            # Elimina los puntos de los números
            df.loc[no_nulos, columna] = df.loc[no_nulos, columna].str.replace('.', '')
            # Elimina porcentajes y comas
            df.loc[no_nulos, columna] = df.loc[no_nulos, columna].replace({'%': '', ',': '.', r'[^\d.]':''}, regex=True)
            # Cambia tipo de dato a numerico
            df[columna] = pd.to_numeric(df[columna], errors='coerce')
        
        df['municipio'] = df['municipio'].astype(str)


