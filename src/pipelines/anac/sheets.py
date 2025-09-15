import logging
import os
import pandas as pd
from src.shared.sheets import ConexionGoogleSheets

def load_anac_sheets_data(datos_nuevos, df):

    logger = logging.getLogger("anac")
    logger.info("💾 Iniciando carga al Google Sheets...")

    if not datos_nuevos:
        logger.info("⚠️ No hay datos nuevos para cargar al Sheets.")
        print("⛔ datos_nuevos es False")
        return
    
    # Filtrar solo Corrientes
    df_corrientes = df[df["aeropuerto"].str.upper().str.contains("CORRIENTES")]
    if df_corrientes.empty:
        logger.warning("⚠️ No hay registros de Corrientes en el DataFrame.")
        return

    # Obtener el valor más reciente
    df_corrientes["fecha"] = pd.to_datetime(df_corrientes["fecha"])
    fila_mas_reciente = df_corrientes.sort_values("fecha").iloc[-1]
    cantidad = fila_mas_reciente["cantidad"]

    SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'

    try:
        sheets = ConexionGoogleSheets(SPREADSHEET_ID)
        sheets.escribir_valor_en_columna_siguiente(fila=10, valor=cantidad, sheet_name="Datos")
        print("📤 Valor de pasajeros en aeropuerto Corrientes escrito en Google Sheets.")
        logger.info("✅ Carga a Google Sheets completada.")
    except Exception as e:
        print(f"❌ Excepción: {e}")
        logger.error(f"❌ Error durante la carga a Google Sheets: {e}")