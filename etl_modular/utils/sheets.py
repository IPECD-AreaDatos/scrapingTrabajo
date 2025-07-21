import os
import logging
import pandas as pd
from json import loads
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2 import service_account

class ConexionGoogleSheets():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    def __init__(self, spreadsheet_id: str, env_var: str = 'GOOGLE_SHEETS_API_KEY') -> None:
        self.spreadsheet_id = spreadsheet_id
        raw_json = os.getenv(env_var)

        if not raw_json:
            raise ValueError(f"No se encontró la variable de entorno '{env_var}'.")

        try:
            # Doble loads en caso de cadena JSON escapada
            key_dict = loads(raw_json)
            if 'private_key' in key_dict:
                key_dict['private_key'] = key_dict['private_key'].replace('\\n', '\n')

            if isinstance(key_dict, str):
                print("Se detectó string después del primer loads, haciendo loads de nuevo")
                key_dict = loads(key_dict)

            self.creds = service_account.Credentials.from_service_account_info(key_dict, scopes=self.SCOPES)
            self.service = build('sheets', 'v4', credentials=self.creds).spreadsheets()
            self.logger = logging.getLogger(__name__)
            self.logger.info("✅ Google Sheets API inicializado correctamente.")
        except Exception as e:
            raise ValueError(f"Error al procesar las credenciales de Google Sheets: {e}")

    def leer_df(self, rango: str, header: bool = True) -> pd.DataFrame:
        result = self.service.values().get(spreadsheetId=self.spreadsheet_id, range=rango).execute()
        values = result.get('values', [])
        if not values:
            self.logger.warning(f"⚠️ Rango vacío: {rango}")
            return pd.DataFrame()
        if header:
            df = pd.DataFrame(values[1:], columns=values[0])
        else:
            df = pd.DataFrame(values)
        self.logger.info(f"📥 Datos leídos del rango {rango}: {len(df)} filas")
        return df

    def escribir_df(self, df: pd.DataFrame, rango_inicio: str, clear: bool = True):
        if clear:
            self.service.values().clear(spreadsheetId=self.spreadsheet_id, range=rango_inicio).execute()
        body = {'values': [df.columns.tolist()] + df.values.tolist()}
        response = self.service.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=rango_inicio,
            valueInputOption='RAW',
            body=body
        ).execute()
        self.logger.info(f"📤 Datos escritos en {rango_inicio}: {response.get('updatedCells')} celdas")

    def append_df(self, df: pd.DataFrame, rango: str):
        body = {'values': df.values.tolist()}
        response = self.service.values().append(
            spreadsheetId=self.spreadsheet_id,
            range=rango,
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()
        self.logger.info(f"📎 Datos agregados en {rango}: {response.get('updates', {}).get('updatedCells')} celdas")

    def get_last_col_letter(self, fila: int = 1) -> str:
        # Obtener la última columna realmente ocupada (con datos no vacíos)
        result = self.service.values().get(
            spreadsheetId=self.spreadsheet_id,
            range=f'{fila}:{fila}'
        ).execute()
        values = result.get('values', [])
        
        last_col_index = 0
        if values and values[0]:
            for i, cell in enumerate(values[0]):
                if str(cell).strip() != "":
                    last_col_index = i + 1  # índice 1-based
        
        if last_col_index == 0:
            last_col_index = 1  # Si no hay datos, usar la columna A
        
        next_col_letter = self.num_to_col(last_col_index + 1)  # Columna siguiente vacía
        return next_col_letter

    def escribir_valor_en_columna_siguiente(self, fila: int, valor):
        col_letter = self.get_last_col_letter(fila)
        rango = f'{col_letter}{fila}'
        response = self.service.values().update(
            spreadsheetId=self.spreadsheet_id,
            range=rango,
            valueInputOption='RAW',
            body={'values': [[valor]]}
        ).execute()
        self.logger.info(f"✅ Valor {valor} escrito en {rango}: {response.get('updatedCells')} celdas")
        
    @staticmethod
    def num_to_col(n: int) -> str:
        """Convierte índice 1-based a letra de columna (ej: 1 -> A, 27 -> AA)"""
        result = ""
        n -= 1  # ajustar a 0-based
        while n >= 0:
            result = chr(n % 26 + 65) + result
            n = n // 26 - 1
        return result

    def transformar_numericos(self, df: pd.DataFrame, columnas: list):
        for col in columnas:
            df[col] = df[col].astype(str).str.replace('.', '', regex=False)
            df[col] = df[col].replace({'%': '', ',': '.', r'[^\d.]':''}, regex=True)
            df[col] = pd.to_numeric(df[col], errors='coerce')
        return df