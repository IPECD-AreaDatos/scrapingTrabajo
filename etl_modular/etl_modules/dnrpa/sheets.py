import pandas as pd
import logging
import os
from dotenv import load_dotenv
from etl_modular.utils.logger import setup_logger
from etl_modular.utils.db import ConexionBaseDatos
from etl_modular.utils.sheets import ConexionGoogleSheets # Importa la clase de conexión a Google Sheets desde utils

logger = logging.getLogger(__name__)

def run_dnrpa_sheets_update():
    """
    Orquesta el proceso de extracción de datos de DNRPA desde la base de datos
    y su carga en Google Sheets (Corrientes, NEA y Nacional).
    """
    setup_logger("dnrpa_sheets")
    logger.info("Iniciando el proceso de actualización de Google Sheets para DNRPA.")

    load_dotenv()
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')
    
    spreadsheet_id = os.getenv('GOOGLE_SHEETS_DNRPA_SPREADSHEET_ID') 

    if not spreadsheet_id:
        logger.error("GOOGLE_SHEETS_DNRPA_SPREADSHEET_ID no está configurado en el archivo .env. Por favor, añádelo.")
        return

    conexion_db = None
    sheets_client = None

    try:
        # 1. Conectar a la base de datos
        conexion_db = ConexionBaseDatos(host, user, password, database)
        conexion_db.connect_db()
        logger.info("Conexión a la base de datos establecida.")

        # 2. Inicializar cliente de Google Sheets
        sheets_client = ConexionGoogleSheets(spreadsheet_id=spreadsheet_id)
        logger.info("Cliente de Google Sheets inicializado.")

        # 3. Extraer y cargar datos para Corrientes (ID 18)
        autos_ctes, motos_ctes = get_dnrpa_data_for_province(conexion_db, provincia_id=18)
        if not autos_ctes or not motos_ctes:
            logger.warning("No se obtuvieron datos para Corrientes. Omitiendo carga a Sheets de Corrientes.")
        else:
            load_dnrpa_sheets_corrientes(sheets_client, autos_ctes, motos_ctes, spreadsheet_id)

        # 4. Extraer datos para provincias del NEA y Nación
        autos_ch, motos_ch = get_dnrpa_data_for_province(conexion_db, provincia_id=22) # Chaco
        autos_ms, motos_ms = get_dnrpa_data_for_province(conexion_db, provincia_id=54) # Misiones
        autos_fs, motos_fs = get_dnrpa_data_for_province(conexion_db, provincia_id=34) # Formosa
        autos_nc, motos_nc = get_dnrpa_data_for_nation(conexion_db) # Nación (suma de todas las provincias)

        # 5. Cargar datos del NEA y Nación a Google Sheets
        load_dnrpa_sheets_nea_and_nation(
            sheets_client,
            data_ch=(autos_ch, motos_ch),
            data_ms=(autos_ms, motos_ms),
            data_fs=(autos_fs, motos_fs),
            data_nc=(autos_nc, motos_nc),
            spreadsheet_id=spreadsheet_id
        )
        
        logger.info("Proceso de actualización de Google Sheets para DNRPA finalizado exitosamente.")

    except Exception as e:
        logger.error(f"❌ Error crítico en el proceso de actualización de Google Sheets para DNRPA: {e}", exc_info=True)
    finally:
        if conexion_db:
            conexion_db.close_connections()
            logger.info("Conexión a la base de datos cerrada.")
        logger.info("Proceso de actualización de Google Sheets para DNRPA terminado.")

# --- FUNCIONES DE EXTRACCIÓN DE DATOS DE LA BASE DE DATOS PARA SHEETS ---

def get_dnrpa_data_for_province(conexion_db, provincia_id: int):
    logger.info(f"Extrayendo datos de DNRPA para provincia ID {provincia_id} para Sheets...")
    
    try:
        # Asegúrate de que la conexión a la DB esté activa y sea válida
        if conexion_db is None or conexion_db.conn is None:
            logger.error("La conexión a la base de datos no está disponible.")
            return [], []

        df = pd.read_sql('SELECT fecha, id_provincia_indec, id_vehiculo, cantidad from dnrpa', conexion_db.conn)
        df['fecha'] = pd.to_datetime(df['fecha'])

        # Filtrar por provincia y fecha
        df_filtered = df[(df['id_provincia_indec'] == provincia_id) & (df['fecha'] >= '2018-12-01')].copy()

        # Crear listas para autos y motos
        autos = df_filtered['cantidad'][(df_filtered['id_vehiculo'] == 1)].tolist()
        motos = df_filtered['cantidad'][(df_filtered['id_vehiculo'] == 2)].tolist()

        # Eliminar ceros de las listas (como en tu código original)
        autos = [dato for dato in autos if dato != 0]
        motos = [dato for dato in motos if dato != 0]

        logger.info(f"Datos extraídos para Sheets (Provincia ID: {provincia_id}).")
        print(f"DEBUG: Datos de autos para provincia {provincia_id}: {autos[:5]}... ({len(autos)} elementos)") # DEBUG
        print(f"DEBUG: Datos de motos para provincia {provincia_id}: {motos[:5]}... ({len(motos)} elementos)") # DEBUG
        return [autos], [motos] # Retorna listas de listas como espera Google Sheets API
    except Exception as e:
        logger.error(f"Error extrayendo datos de DNRPA para provincia ID {provincia_id} para Sheets: {e}", exc_info=True)
        return [], [] # Retorna listas vacías en caso de error

def get_dnrpa_data_for_nation(conexion_db):
    logger.info("Extrayendo datos de DNRPA a nivel nacional para Sheets...")
    
    try:
        # Asegúrate de que la conexión a la DB esté activa y sea válida
        if conexion_db is None or conexion_db.conn is None:
            logger.error("La conexión a la base de datos no está disponible.")
            return [], []

        df = pd.read_sql('SELECT fecha, id_vehiculo, cantidad from dnrpa', conexion_db.conn)
        df['fecha'] = pd.to_datetime(df['fecha'])

        df_filtered = df[df['fecha'] >= '2018-12-01'].copy()

        # Agrupar por mes y sumar para obtener totales nacionales
        autos_nacion = df_filtered[df_filtered['id_vehiculo'] == 1].groupby(pd.Grouper(key='fecha', freq='MS'))['cantidad'].sum().tolist()
        motos_nacion = df_filtered[df_filtered['id_vehiculo'] == 2].groupby(pd.Grouper(key='fecha', freq='MS'))['cantidad'].sum().tolist()

        # Eliminar ceros de las listas
        autos_nacion = [dato for dato in autos_nacion if dato != 0]
        motos_nacion = [dato for dato in motos_nacion if dato != 0]

        logger.info("Datos extraídos para Sheets (Nación).")
        print(f"DEBUG: Datos de autos para Nación: {autos_nacion[:5]}... ({len(autos_nacion)} elementos)") # DEBUG
        print(f"DEBUG: Datos de motos para Nación: {motos_nacion[:5]}... ({len(motos_nacion)} elementos)") # DEBUG
        return [autos_nacion], [motos_nacion] # Retorna listas de listas
    except Exception as e:
        logger.error(f"Error extrayendo datos de DNRPA a nivel nacional para Sheets: {e}", exc_info=True)
        return [], [] # Retorna listas vacías

# --- FUNCIONES DE CARGA DE DATOS A GOOGLE SHEETS ---

def load_dnrpa_sheets_corrientes(sheets_client: ConexionGoogleSheets, autos_data: list, motos_data: list, spreadsheet_id: str):
    logger.info("Cargando datos de Corrientes a Google Sheets...")
    print(f"DEBUG: Cargando Corrientes Autos en {spreadsheet_id} Datos!C7:7 con valores: {autos_data}") # DEBUG
    print(f"DEBUG: Cargando Corrientes Motos en {spreadsheet_id} Datos!C8:8 con valores: {motos_data}") # DEBUG
    try:
        sheets_client.update_sheet_range(
            spreadsheet_id=spreadsheet_id,
            sheet_range='Datos!C7:7',
            values=autos_data
        )
        sheets_client.update_sheet_range(
            spreadsheet_id=spreadsheet_id,
            sheet_range='Datos!C8:8',
            values=motos_data
        )
        logger.info("Datos de Corrientes cargados exitosamente en Google Sheets.")
    except Exception as e:
        logger.error(f"Error cargando datos de Corrientes a Google Sheets: {e}", exc_info=True)
        raise # Re-lanza la excepción

def load_dnrpa_sheets_nea_and_nation(sheets_client: ConexionGoogleSheets, data_ch: tuple, data_ms: tuple, data_fs: tuple, data_nc: tuple, spreadsheet_id: str):
    logger.info("Cargando datos del NEA y Nación a Google Sheets...")
    print(f"DEBUG: Cargando Chaco Autos en {spreadsheet_id} Patentamiento Autos!B2:2 con valores: {data_ch[0]}") # DEBUG
    print(f"DEBUG: Cargando Misiones Autos en {spreadsheet_id} Patentamiento Autos!B3:3 con valores: {data_ms[0]}") # DEBUG
    print(f"DEBUG: Cargando Formosa Autos en {spreadsheet_id} Patentamiento Autos!B4:4 con valores: {data_fs[0]}") # DEBUG
    print(f"DEBUG: Cargando Nación Autos en {spreadsheet_id} Patentamiento Autos!B5:5 con valores: {data_nc[0]}") # DEBUG
    print(f"DEBUG: Cargando Chaco Motos en {spreadsheet_id} Patentamiento Motos!B2:2 con valores: {data_ch[1]}") # DEBUG
    print(f"DEBUG: Cargando Misiones Motos en {spreadsheet_id} Patentamiento Motos!B3:3 con valores: {data_ms[1]}") # DEBUG
    print(f"DEBUG: Cargando Formosa Motos en {spreadsheet_id} Patentamiento Motos!B4:4 con valores: {data_fs[1]}") # DEBUG
    print(f"DEBUG: Cargando Nación Motos en {spreadsheet_id} Patentamiento Motos!B5:5 con valores: {data_nc[1]}") # DEBUG

    try:
        # Patentamiento Autos
        sheets_client.update_sheet_range(spreadsheet_id=spreadsheet_id, sheet_range='Patentamiento Autos!B2:2', values=data_ch[0])
        sheets_client.update_sheet_range(spreadsheet_id=spreadsheet_id, sheet_range='Patentamiento Autos!B3:3', values=data_ms[0])
        sheets_client.update_sheet_range(spreadsheet_id=spreadsheet_id, sheet_range='Patentamiento Autos!B4:4', values=data_fs[0])
        sheets_client.update_sheet_range(spreadsheet_id=spreadsheet_id, range='Patentamiento Autos!B5:5', values=data_nc[0])

        # Patentamiento Motos
        sheets_client.update_sheet_range(spreadsheet_id=spreadsheet_id, sheet_range='Patentamiento Motos!B2:2', values=data_ch[1])
        sheets_client.update_sheet_range(spreadsheet_id=spreadsheet_id, sheet_range='Patentamiento Motos!B3:3', values=data_ms[1])
        sheets_client.update_sheet_range(spreadsheet_id=spreadsheet_id, sheet_range='Patentamiento Motos!B4:4', values=data_fs[1])
        sheets_client.update_sheet_range(spreadsheet_id=spreadsheet_id, range='Patentamiento Motos!B5:5', values=data_nc[1])
        
        logger.info("Datos del NEA y Nación cargados exitosamente en Google Sheets.")
    except Exception as e:
        logger.error(f"Error cargando datos del NEA y Nación a Google Sheets: {e}", exc_info=True)
        raise # Re-lanza la excepción