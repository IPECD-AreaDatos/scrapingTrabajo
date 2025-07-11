import os
from dotenv import load_dotenv
from etl_modular.utils.logger import setup_logger
from .extract import extract_mercado_central_data
from .transform import transform_mercado_central_data

def run_mercado_central():
    setup_logger("mercado_central")

    load_dotenv()
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    #extract_mercado_central_data()
    df = transform_mercado_central_data()
    print("final")
    print(df)
   