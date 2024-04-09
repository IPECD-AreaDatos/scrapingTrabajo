import os
from google.oauth2 import service_account
from googleapiclient.discovery import build


# Define los alcances y la ruta al archivo JSON de credenciales
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
KEY = os.path.join(ruta_carpeta_files, 'key.json')

# Escribe aquí el ID de tu documento:
SPREADSHEET_ID = '1HnK6eMrd_P6V8P141WPZ0jz2ivoO6opyX0YWy342fRM'

# Carga las credenciales desde el archivo JSON
creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)

# Crea una instancia de la API de Google Sheets
service = build('sheets', 'v4', credentials=creds)
sheet = service.spreadsheets()

#Realiza una llamada a la API para obtener datos desde la hoja 'Hoja 1' en el rango 'B2:Z12'
result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range='Educacion_ECV!B2:Z12').execute()
# Extrae los valores del resultado
values = result.get('values', [])[1:]