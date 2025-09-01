import os
from dotenv import load_dotenv
from etl_modular.utils.db import ConexionBaseDatos

load_dotenv()

host = os.getenv("HOST_DBB")
user = os.getenv("USER_DBB")
password = os.getenv("PASSWORD_DBB")
database = os.getenv("NAME_DBB_DATALAKE_ECONOMICO")

def load_supermercado_data(df, db: ConexionBaseDatos):

    exito = db.load_if_newer(
        df,
        table_name="supermercado_encuesta",
        date_column="fecha"
    )

    return exito
