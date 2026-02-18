"""
EXTRACT - Módulo de extracción de datos IPICORR
Responsabilidad: Leer datos desde Google Sheets
"""
import os
import logging
import pandas as pd
from json import loads
from google.oauth2 import service_account
from googleapiclient.discovery import build

logger = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '19rIG_9MBTq3QxCWArnRYnx8O8BYt4uqJTsISYHC02Pg'
RANGO = '3. Datos para tablero'
COLUMNAS = ['Fecha', 'Var_ia_Nivel_General', 'Vim_Nivel_General', 'Vim_Alimentos',
            'Vim_Textil', 'Vim_Maderas', 'Vim_min_nometalicos', 'Vim_metales',
            'Var_ia_Alimentos', 'Var_ia_Textil', 'Var_ia_Maderas',
            'Var_ia_min_nometalicos', 'Var_ia_metales', 'Estatus']
MESES = {
    'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04', 'may': '05', 'jun': '06',
    'jul': '07', 'ago': '08', 'sept': '09', 'oct': '10', 'nov': '11', 'dic': '12'
}


class ExtractIPICORR:
    """Lee los datos del IPICORR desde Google Sheets."""

    def extract(self) -> pd.DataFrame:
        """
        Lee la hoja de Google Sheets y retorna un DataFrame limpio.

        Returns:
            pd.DataFrame con columnas del IPICORR
        """
        logger.info("[EXTRACT] Conectando a Google Sheets (IPICORR)...")
        key_dict = loads(os.getenv('GOOGLE_SHEETS_API_KEY'))
        creds = service_account.Credentials.from_service_account_info(key_dict, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGO).execute()
        values = result.get('values', [])
        values.pop(1)  # Elimina fila con [fecha] sola
        values.pop(0)  # Elimina títulos

        filas = [row for row in values if len(row) == 14]
        df = pd.DataFrame(filas, columns=COLUMNAS)
        df['Fecha'] = df['Fecha'].apply(self._convertir_fecha)
        df = df.drop('Estatus', axis=1)
        df = df.iloc[11:].reset_index(drop=True)

        logger.info("[EXTRACT] IPICORR: %d filas leídas.", len(df))
        return df

    def _convertir_fecha(self, fecha_str: str) -> str:
        partes = fecha_str.split('-')
        mes_num = MESES.get(partes[0].lower(), '00')
        return f"{partes[1]}-{mes_num}-01"
