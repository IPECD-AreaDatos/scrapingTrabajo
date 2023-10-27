from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Define los alcances y la ruta al archivo JSON de credenciales
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
KEY = 'C:\\Users\\Usuario\\Desktop\\scrapingTrabajo\\scrap_IPICORR\\files\\key.json'  # Asegúrate de usar doble barra invertida en la ruta

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

# Imprime los valores
for row in values:
    print(row)