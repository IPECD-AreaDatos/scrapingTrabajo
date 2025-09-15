import logging
import os
from src.shared.sheets import ConexionGoogleSheets

def load_combustible_sheets_data(datos_nuevos, suma_mensual):
    logger = logging.getLogger("combustible")
    logger.info("💾 Iniciando carga al Google Sheets...")

    if not datos_nuevos:
        logger.info("⚠️ No hay datos nuevos para cargar al Sheets.")
        print("⛔ datos_nuevos es False")
        return
    
    print("✅ datos_nuevos es True, creando objeto ConexionGoogleSheets")
    SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'

    try:
        sheets = ConexionGoogleSheets(SPREADSHEET_ID)
        print("📄 Objeto sheets creado")
        sheets.escribir_valor_en_columna_siguiente(fila=6, valor=suma_mensual, sheet_name="Datos")
        print("📤 Valor escrito en columna")
        logger.info("✅ Carga a Google Sheets completada.")
    except Exception as e:
        print(f"❌ Excepción: {e}")
        logger.error(f"❌ Error durante la carga a Google Sheets: {e}")