import os
import logging
import pandas as pd
from json import loads
from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# ID de la nueva hoja de Salud - Embarazo
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID_SALUD') 
RANGO = 'EmbarazadasAR!A:AZ' 

class Extract:
    """Lee los datos de embarazo desde Google Sheets."""

    def extract(self) -> pd.DataFrame:
        logger.info("[EXTRACT] Conectando a Google Sheets (Salud)...")
        
        key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))
        creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGO).execute()
        values = result.get('values', [])

        if not values:
            logger.warning("[EXTRACT] No se encontraron datos en la hoja.")
            return pd.DataFrame()

        # Usamos la primera fila como encabezados
        headers = values[0]
        data = values[1:]
        
        # Filtramos filas vacías (chequeamos que tengan al menos el DNI o Apellido)
        df = pd.DataFrame(data, columns=headers)
        df = df[df['DNI'].notna() & (df['DNI'] != '')]

        logger.info("[EXTRACT] Salud: %d filas leídas.", len(df))
        return df