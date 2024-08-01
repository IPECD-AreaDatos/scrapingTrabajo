from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
import pandas as pd

"""
    readSheets.py
    Objetivo: Obtener los datos de una hoja de Google Sheets(Datos para tablero)  y convertirlos en un dataframe
    Hoja de Google Sheets: https://docs.google.com/spreadsheets/d/1NGcF5fXO7RCXIRGJ2UQO98x_T_tZtwHTnvD-RmTdV0E/edit?gid=1980918856#gid=1980918856
"""

class readSheets:
    def tratar_datos(self):
        df = []
        # Define los alcances y la ruta al archivo JSON de credenciales
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

        #Direccion del archivo json
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        KEY = os.path.join(ruta_carpeta_files, 'key.json')

        #ID del documento:
        SPREADSHEET_ID = '1NGcF5fXO7RCXIRGJ2UQO98x_T_tZtwHTnvD-RmTdV0E'

        # Carga las credenciales desde el archivo JSON
        creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)

        # Crea una instancia de la API de Google Sheets
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Realiza una llamada a la API para obtener datos desde de la hoja
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Datos para tablero').execute()

        # Extrae los valores del resultado
        values = result.get('values', [])
        #Elimina la segunda fila que tiene [fecha] sola
        values.pop(1)
        #Elimina los titulos para despues agregar al df 
        values.pop(0)

        # Imprime los valores
        for row in values:
            if len(row)== 8: #Si tiene el Finalizado se agrega al df
                print(row)
                df.append(row)
        
        #Se crea el Data Frame
        df = pd.DataFrame(df, columns=['Fecha', 'Var_Interanual_IPICORR', 'Var_Interanual_Alimentos', 'Var_Interanual_Textil', 'Var_Interanual_Maderas', 'Var_Interanual_MinNoMetalicos', 'Var_Interanual_Metales', 'Estado'])
        df['Fecha'] = df['Fecha'].apply(convertir_fecha) #Se transforma el formato fecha de sept-2022 a 01/09/2022
       
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