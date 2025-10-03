import os
from dotenv import load_dotenv
import pymysql
from sqlalchemy import create_engine
import pandas as pd
import logging

load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

host = os.getenv("HOST_DBB")
user = os.getenv("USER_DBB")
password = os.getenv("PASSWORD_DBB")
database = os.getenv("NAME_DBB_DATALAKE_ECONOMICO")

# Importar la implementación local
from .utils_db import ConexionBaseDatos
def load_canasta_basica_data(df):
    print("Iniciando carga a base de datos ( reemplazando los datos existentes)...")
    conexion = ConexionBaseDatos(host, user, password, database)
    conexion.connect_db()

    exito = conexion.insert_append(
        table_name="canasta_basica",
        df=df
    )

    conexion.close_connections()

    if exito:
        print("Datos nuevos cargados correctamente en la tabla canasta basica.")
    else:
        print("No se detectaron datos más recientes. No se cargó nada.")

    return exito