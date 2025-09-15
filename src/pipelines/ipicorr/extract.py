from src.shared.sheets import ConexionGoogleSheets
import pandas as pd
from src.shared.logger import setup_logger
from dotenv import load_dotenv

logger = setup_logger("ipicorr")

def extract_ipicorr():
    load_dotenv()

    SHEET_ID = '19rIG_9MBTq3QxCWArnRYnx8O8BYt4uqJTsISYHC02Pg'
    RANGO = '3. Datos para tablero'

    gs = ConexionGoogleSheets(SHEET_ID)
    df_raw = gs.leer_df(RANGO, header=False)

    # Limpieza inicial: eliminamos fila 0 (títulos) y fila 1 (solo tiene fecha)
    df_raw = df_raw.drop([0, 1]).reset_index(drop=True)

    return df_raw