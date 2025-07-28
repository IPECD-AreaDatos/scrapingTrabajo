import logging
import os
import pandas as pd
from etl_modular.utils.sheets import ConexionGoogleSheets

def load_dnrpa_sheets_data(datos_nuevos, df):
    logger = logging.getLogger("dnrpa")
    logger.info("💾 Iniciando carga al Google Sheets...")

    if not datos_nuevos:
        logger.info("⚠️ No hay datos nuevos para cargar al Sheets.")
        print("⛔ datos_nuevos es False")
        return

    # Convertir columna fecha a datetime
    df["fecha"] = pd.to_datetime(df["fecha"])

    # Filtrar registros desde diciembre 2018 en adelante
    df = df[df["fecha"] >= "2018-12-01"]

    # Crear listas por provincia y tipo de vehículo
    def filtrar_y_extraer(provincia_id, vehiculo_id):
        filtrado = df[(df["id_provincia_indec"] == provincia_id) & (df["id_vehiculo"] == vehiculo_id)]
        cantidades = filtrado["cantidad"].tolist()
        return [list(filter(lambda x: x != 0, cantidades))]

    # Corrientes (ID 18)
    autos_ctes = filtrar_y_extraer(18, 1)
    motos_ctes = filtrar_y_extraer(18, 2)

    # Chaco (22), Misiones (54), Formosa (34), Nacional
    autos_ch = filtrar_y_extraer(22, 1)
    motos_ch = filtrar_y_extraer(22, 2)
    autos_ms = filtrar_y_extraer(54, 1)
    motos_ms = filtrar_y_extraer(54, 2)
    autos_fs = filtrar_y_extraer(34, 1)
    motos_fs = filtrar_y_extraer(34, 2)

    # Nacional: agrupar por mes
    autos_nc = [df[df["id_vehiculo"] == 1].groupby(pd.Grouper(key="fecha", freq="MS"))["cantidad"].sum().tolist()]
    motos_nc = [df[df["id_vehiculo"] == 2].groupby(pd.Grouper(key="fecha", freq="MS"))["cantidad"].sum().tolist()]

    SPREADSHEET_ID = '1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI'

    try:
        sheets = ConexionGoogleSheets(SPREADSHEET_ID)

        # Corrientes (hoja "Datos")
        sheets.escribir_valor_en_columna_siguiente(fila=7, valor=autos_ctes[0][-1], sheet_name="Datos")
        sheets.escribir_valor_en_columna_siguiente(fila=8, valor=motos_ctes[0][-1], sheet_name="Datos")

        # NEA - Autos (hoja "Patentamiento Autos")
        sheets.escribir_valor_en_columna_siguiente(fila=2, valor=autos_ch[0][-1], sheet_name="Patentamiento Autos")
        sheets.escribir_valor_en_columna_siguiente(fila=3, valor=autos_ms[0][-1], sheet_name="Patentamiento Autos")
        sheets.escribir_valor_en_columna_siguiente(fila=4, valor=autos_fs[0][-1], sheet_name="Patentamiento Autos")
        sheets.escribir_valor_en_columna_siguiente(fila=5, valor=autos_nc[0][-1], sheet_name="Patentamiento Autos")

        # NEA - Motos (hoja "Patentamiento Motos")
        sheets.escribir_valor_en_columna_siguiente(fila=2, valor=motos_ch[0][-1], sheet_name="Patentamiento Motos")
        sheets.escribir_valor_en_columna_siguiente(fila=3, valor=motos_ms[0][-1], sheet_name="Patentamiento Motos")
        sheets.escribir_valor_en_columna_siguiente(fila=4, valor=motos_fs[0][-1], sheet_name="Patentamiento Motos")
        sheets.escribir_valor_en_columna_siguiente(fila=5, valor=motos_nc[0][-1], sheet_name="Patentamiento Motos")

        print("📤 Valor más reciente de DNRPA escrito en Google Sheets.")
        logger.info("✅ Carga a Google Sheets completada.")
    
    except Exception as e:
        print(f"❌ Excepción: {e}")
        logger.error(f"❌ Error durante la carga a Google Sheets: {e}")