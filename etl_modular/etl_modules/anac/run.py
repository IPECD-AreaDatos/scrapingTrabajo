import os
from dotenv import load_dotenv
from etl_modular.utils.logger import setup_logger
from .extract import extract_anac_data
from .transform import transform_anac_data
from .load import load_anac_data
from .sheets import load_anac_sheets_data

def run_anac():
    setup_logger("anac")

    load_dotenv()
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    ruta = extract_anac_data()
    #ruta = "etl_modular/data/raw/ANAC.xlsx"
    df = transform_anac_data(ruta)
    print(df)
    datos_nuevos = load_anac_data(df)
    print(datos_nuevos)

    load_anac_sheets_data(datos_nuevos, df)