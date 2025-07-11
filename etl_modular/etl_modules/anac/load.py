import os
from dotenv import load_dotenv
from etl_modular.utils.db import ConexionBaseDatos

load_dotenv()

host = os.getenv("HOST_DBB")
user = os.getenv("USER_DBB")
password = os.getenv("PASSWORD_DBB")
database = os.getenv("NAME_DBB_DATALAKE_ECONOMICO")

def load_anac_data(df):
    print("💾 Iniciando carga a base de datos ( reemplazando los datos existentes)...")
    conexion = ConexionBaseDatos(host, user, password, database)
    conexion.connect_db()

    exito = conexion.load_if_newer(
        df,
        table_name="anac",
        date_column='fecha'
    )

    conexion.close_connections()

    if exito:
        print("✅ Datos nuevos cargados correctamente en la tabla anac.")
    else:
        print("⚠️ No se detectaron datos más recientes. No se cargó nada.")

    return exito
