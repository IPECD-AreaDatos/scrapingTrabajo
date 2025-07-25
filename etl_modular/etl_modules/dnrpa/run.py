import os
from dotenv import load_dotenv
from etl_modular.utils.logger import setup_logger
from etl_modular.etl_modules.dnrpa.extract import extract_dnrpa_data
from etl_modular.etl_modules.dnrpa.transform import transform_dnrpa_data
from etl_modular.etl_modules.dnrpa.load import load_dnrpa_data
from etl_modular.etl_modules.dnrpa.sheets import run_dnrpa_sheets_update
from etl_modular.utils.db import ConexionBaseDatos

def run_dnrpa(mode='last'):
    setup_logger("dnrpa")

    load_dotenv()
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    conexion_db = None
    # Bandera para indicar si se cargaron nuevos datos del último año
    should_update_sheets = False 

    try:
        conexion_db = ConexionBaseDatos(host, user, password, database)
        conexion_db.connect_db()

        # Lógica para CARGA HISTÓRICA (solo si mode es 'historical')
        if mode == 'historical':
            print("\n--- INICIANDO CARGA HISTÓRICA ---")
            raw_data_historical = extract_dnrpa_data(mode='historical')
            if raw_data_historical: # Solo transformar y cargar si hay datos extraídos
                df_transformed_historical = transform_dnrpa_data(raw_data_historical)
                load_dnrpa_data(df_transformed_historical, conexion_db) 
                print("✅ Carga histórica de DNRPA finalizada.")
            else:
                print("⚠️ No se extrajeron datos históricos, omitiendo la carga histórica.")
        
        # Lógica para CARGA DEL ÚLTIMO AÑO (por defecto o si mode es 'last')
        elif mode == 'last': # Usamos 'elif' para que sea mutuamente excluyente con 'historical'
            print("\n--- INICIANDO CARGA DEL ÚLTIMO AÑO ---")
            raw_data_last = extract_dnrpa_data(mode='last')
            if raw_data_last: # Solo transformar y cargar si hay datos extraídos
                df_transformed_last = transform_dnrpa_data(raw_data_last)
                if not df_transformed_last.empty: # Solo cargar si el DataFrame no está vacío
                    # Realiza la carga a la base de datos.
                    # El valor de retorno de load_dnrpa_data no se usa aquí para la bandera de Sheets.
                    load_dnrpa_data(df_transformed_last, conexion_db) 
                    print("✅ Carga del último año de DNRPA finalizada.")
                    
                    # *** CAMBIO CLAVE AQUÍ ***
                    # Si llegamos hasta aquí y el DataFrame transformado no está vacío,
                    # asumimos que la data del último año fue procesada y debería intentar actualizar Sheets.
                    should_update_sheets = True 
                else:
                    print("⚠️ Datos del último año transformados vacíos, omitiendo carga a DB.")
            else:
                print("⚠️ No se extrajeron datos del último año, omitiendo la carga del último año.")
        else:
            print(f"⚠️ Modo '{mode}' no reconocido. Use 'last' o 'historical'.")
    
        # Ejecuta condicionalmente la actualización de Sheets
        # Esta lógica se ejecuta al final del try, después de ambos modos (si se usan).
        if should_update_sheets:
            print("\n--- INICIANDO ACTUALIZACIÓN DE GOOGLE SHEETS (Datos del último año procesados) ---")
            run_dnrpa_sheets_update()
            print("✅ Actualización de Google Sheets finalizada.")
        else:
            print("\n--- OMITIENDO ACTUALIZACIÓN DE GOOGLE SHEETS (No se procesaron datos del último año) ---")
    
    except Exception as e:
        print(f"❌ Error en el proceso ETL de DNRPA: {e}")
    finally:
        if conexion_db:
            conexion_db.close_connections()