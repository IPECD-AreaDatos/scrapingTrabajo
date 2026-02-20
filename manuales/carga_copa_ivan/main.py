import os
import sys

# Permite ejecutar desde cualquier directorio
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
from read_csv import readSheets
from transformData import transformData
from conectDataBase import conectDataBase

# Cargar variables de entorno desde el .env del proyecto raíz
dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".env")
load_dotenv(dotenv_path=dotenv_path)

host_dbb  = os.getenv("HOST_DBB")
user_dbb  = os.getenv("USER_DBB")
pass_dbb  = os.getenv("PASSWORD_DBB")
dbb_datalake = os.getenv("NAME_DBB_DATALAKE_ECONOMICO")


def load_copa_data_to_datalake(df):
    """Carga los datos de recursos de origen nacional al datalake."""
    db_connection = conectDataBase(host_dbb, user_dbb, pass_dbb, dbb_datalake)
    if db_connection.connect_db():
        db_connection.load_recursos_origen_nacional(df)
        db_connection.cerrar_conexion()


def main():
    # 1. Leer el CSV
    print("=== Leyendo datos ===")
    df_raw = readSheets().readDataCopa()

    # 2. Transformar los datos
    print("\n=== Transformando datos ===")
    transformer = transformData()
    df_transformed = transformer.processData(df_raw)

    print("\nMuestra de los datos transformados:")
    print(df_transformed.head())
    print(f"\nColumnas: {list(df_transformed.columns)}")

    # 3. Cargar al datalake
    print("\n=== Cargando datos al datalake ===")
    load_copa_data_to_datalake(df_transformed)

    print("\n=== Proceso finalizado exitosamente ===")


if __name__ == "__main__":
    main()
