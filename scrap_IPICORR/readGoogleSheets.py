import os
import pandas as pd
from dotenv import load_dotenv
from json import loads
from google.oauth2 import service_account
from googleapiclient.discovery import build

class readGoogleSheets:
    def tratar_datos(self):
        # Cargamos variables de entorno
        load_dotenv()
        df = []

        # Define los alcances
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        # CARGAMOS LA KEY DE LA API y la convertimos a un JSON, ya que se almacena como str
        key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))

        # Carga las credenciales desde el diccionario JSON
        creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)

        # ID del documento:
        SPREADSHEET_ID = '1NGcF5fXO7RCXIRGJ2UQO98x_T_tZtwHTnvD-RmTdV0E'

        # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Realiza una llamada a la API para obtener datos desde de la hoja
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Datos para tablero').execute()

        # Extrae los valores del resultado
        values = result.get('values', [])
        values.pop(1)  # Elimina la segunda fila que tiene [fecha] sola
        values.pop(0)  # Elimina los títulos para después agregar al df 

        # Imprime los valores y filtra las filas con 8 elementos
        for row in values:
            if len(row) == 8:  # Si tiene el Finalizado se agrega al df
                print(row)
                df.append(row)
        
        # Crea el Data Frame
        df = pd.DataFrame(df, columns=['Fecha', 'Var_Interanual_IPICORR', 'Var_Interanual_Alimentos', 'Var_Interanual_Textil', 'Var_Interanual_Maderas', 'Var_Interanual_MinNoMetalicos', 'Var_Interanual_Metales', 'Estado'])
        df['Fecha'] = df['Fecha'].apply(convertir_fecha)  # Transforma el formato de fecha de 'sept-2022' a '2022-09-01'
       
        # Imprime el DataFrame antes de eliminar la última columna
        print("Antes de eliminar la última columna:")
        print(df)

        # Elimina la última columna
        df = df.drop('Estado', axis=1)

        # Imprime el DataFrame después de eliminar la última columna
        print("\nDespués de eliminar la última columna:")
        print(df)
        return df
    
def convertir_fecha(fecha_str):
    # Mapea los nombres de los meses a números
    meses = {
        'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may': '05', 'jun': '06',
        'jul': '07', 'ago': '08', 'sept': '09', 'oct': '10', 'nov': '11', 'dic': '12'
    }
    
    # Divide la fecha en mes y año
    partes = fecha_str.split('-')
    mes = partes[0].lower()  # Convierte el mes a minúsculas para hacer coincidir con el diccionario
    anio = partes[1]

    # Convierte el mes a su número correspondiente
    mes_numero = meses.get(mes, '00')

    # Formatea la fecha en el formato de MySQL
    fecha_mysql = f"{anio}-{mes_numero}-01"
    return fecha_mysql