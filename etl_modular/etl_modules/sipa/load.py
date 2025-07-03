import os
from dotenv import load_dotenv
from etl_modular.utils.db import ConexionBaseDatos
from etl_modular.etl_modules.sipa.db_analytics import SipaAnalytics

load_dotenv()

host = os.getenv("HOST_DBB")
user = os.getenv("USER_DBB")
password = os.getenv("PASSWORD_DBB")
database = os.getenv("NAME_DBB_DATALAKE_ECONOMICO")

def load_sipa_data(df):
    print("💾 Iniciando carga a base de datos (solo si hay datos nuevos)...")
    conexion = ConexionBaseDatos(host, user, password, database)
    conexion.connect_db()

    exito = conexion.load_if_newer(
        df,
        table_name="sipa_valores",
        date_column="fecha"
    )

    if exito:
        analytics = SipaAnalytics(host, user, password)
        analytics.generar_analytics()

    conexion.close_connections()

    if exito:
        print("✅ Datos nuevos cargados correctamente en sipa_valores y DWH.")
    else:
        print("⚠️ No se detectaron datos más recientes. No se cargó nada.")

    return exito
