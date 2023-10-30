from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd



class readSheets:
    def tratar_datos(self):
        df = []
        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        KEY = os.path.join(ruta_carpeta_files, 'key.json')

        # Escribe aquí el ID de tu documento:
        SPREADSHEET_ID = '1NGcF5fXO7RCXIRGJ2UQO98x_T_tZtwHTnvD-RmTdV0E'

        # Carga las credenciales desde el archivo JSON
        creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)

        # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Realiza una llamada a la API para obtener datos desde la hoja 'Hoja 1' en el rango 'A1:A8'
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Datos para tablero').execute()

        # Extrae los valores del resultado
        values = result.get('values', [])
        values.pop(1)
        values.pop(0)

        # Imprime los valores
        for row in values:
            if len(row)== 7:
                df.append(row)

        df = pd.DataFrame(df, columns=['Fecha', 'Var_Interanual_IPICORR', 'Var_Interanual_Alimentos', 'Var_Interanual_Textil', 'Var_Interanual_Maderas', 'Var_Interanual_MinNoMetalicos', 'Var_Interanual_Metales'])
        df['Fecha'] = df['Fecha'].apply(convertir_fecha)
        return df
    
def convertir_fecha(fecha_str):
    # Mapea los nombres de los meses a números
    meses = {
        'ene': '01',
        'feb': '02',
        'mar': '03',
        'abr': '04',
        'may': '05',
        'jun': '06',
        'jul': '07',
        'ago': '08',
        'sept': '09',
        'oct': '10',
        'nov': '11',
        'dic': '12'
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