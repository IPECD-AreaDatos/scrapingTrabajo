import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from read_data_nbi import readData
# Cargar variables de entorno
load_dotenv()

# Configuración de credenciales
host_dbb = os.getenv('HOST_DBB')
user_dbb = os.getenv('USER_DBB')
pass_dbb = os.getenv('PASSWORD_DBB')
dbb_datalake = os.getenv('NAME_DBB_DWH_SOCIO')

# Crear conexión a la base de datos
engine = create_engine(f"mysql+pymysql://{user_dbb}:{pass_dbb}@{host_dbb}/{dbb_datalake}")

# Función para cargar DataFrame en la base de datos
def cargar_tabla(df, nombre_tabla, engine):
    try:
        df.to_sql(nombre_tabla, con=engine, if_exists='replace', index=False)
        print(f"Tabla '{nombre_tabla}' cargada exitosamente.")
    except Exception as e:
        print(f"Error al cargar la tabla '{nombre_tabla}': {e}")

if __name__ == "__main__":
    # Ruta del archivo Excel
    script_dir = os.path.dirname(os.path.abspath(__file__))
    ruta_excel = os.path.join(script_dir, 'files', 'Datos Nación _ NEA - Censo 2022.xlsx')
    
    excel= readData()
    
    # Leer los DataFrames (sustituyendo las funciones por ejemplo directo)
    df_nbi = excel.read_data_nbi(ruta_excel)
    df_inmat = excel.read_data_inmat(ruta_excel)
    df_agua = excel.read_data_agua(ruta_excel)
    df_combustible = excel.read_data_combustible(ruta_excel)
    df_internet = excel.read_data_internet(ruta_excel)
    df_clima_educativo = excel.read_data_clima_educativo(ruta_excel)
    df_educativo_mayor_25 = excel.read_data_educativo_mayor_25(ruta_excel)
    df_cobertura_salud = excel.read_data_cobertura_salud(ruta_excel)
    
    # Subir los DataFrames a la base de datos
    cargar_tabla(df_nbi, 'censo_nea_nacion_NBI', engine)
    cargar_tabla(df_inmat, 'censo_nea_nacion_INMAT', engine)
    cargar_tabla(df_agua, 'censo_nea_nacion_agua', engine)
    cargar_tabla(df_combustible, 'censo_nea_nacion_combustible', engine)
    cargar_tabla(df_internet, 'censo_nea_nacion_internet', engine)
    cargar_tabla(df_clima_educativo, 'censo_nea_nacion_clima_educativo', engine)
    cargar_tabla(df_educativo_mayor_25, 'censo_nea_nacion_nivel_educativo_mayores_25', engine)
    cargar_tabla(df_cobertura_salud, 'censo_nea_nacion_cobertura_salud', engine)
