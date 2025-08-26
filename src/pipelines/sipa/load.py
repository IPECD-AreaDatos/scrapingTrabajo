import os
from dotenv import load_dotenv
from etl_modular.utils.db import ConexionBaseDatos
from etl_modular.etl_modules.sipa.db_analytics import SipaAnalytics

load_dotenv()

host = os.getenv("HOST_DBB")
user = os.getenv("USER_DBB")
password = os.getenv("PASSWORD_DBB")
database = os.getenv("NAME_DBB_DATALAKE_ECONOMICO")

def load_sipa_data(df, db: ConexionBaseDatos):

    exito = db.load_if_newer(
        df,
        table_name="sipa_valores",
        date_column="fecha"
    )

    if exito:
        analytics = SipaAnalytics(host, user, password)
        analytics.generar_analytics()

    return exito
