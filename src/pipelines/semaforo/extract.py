from etl_modular.utils.sheets import ConexionGoogleSheets
from etl_modular.utils.logger import setup_logger

logger = setup_logger("semaforo")

def extract_semaforo():
    SHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'

    gs = ConexionGoogleSheets(SHEET_ID)

    try:
        df_interanual = gs.leer_df('Semaforo!B2:12', header=None)
        df_intermensual = gs.leer_df('Semaforo Intermensual!B2:12', header=None)
        return df_interanual, df_intermensual
    except Exception as e:
        logger.error(f"❌ Error al extraer datos de Sheets: {e}")
        return None, None