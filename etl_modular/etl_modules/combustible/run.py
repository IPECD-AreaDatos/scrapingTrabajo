from .extract import extract_combustible_data
from .transform import transform_combustible_data
from .transform import suma_por_fecha
from .load import load_combustible_data
from .sheets import load_combustible_sheets_data
from etl_modular.utils.logger import setup_logger

import os 
from dotenv import load_dotenv

def run_combustible():
    setup_logger("combustible")

    load_dotenv()
    host = os.getenv('HOST_DBB')
    user = os.getenv('USER_DBB')
    password = os.getenv('PASSWORD_DBB')
    database = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    #ruta = extract_combustible_data()
    ruta = "etl_modular/data/raw/venta_combustible.csv"
    df = transform_combustible_data(ruta)
    suma_mensual = suma_por_fecha(ruta)
    datos_nuevos = load_combustible_data(df)
    print(datos_nuevos)
    print(f"datos_nuevos: {datos_nuevos} | tipo: {type(datos_nuevos)}")
    print(suma_mensual)
    load_combustible_sheets_data(datos_nuevos, suma_mensual)
