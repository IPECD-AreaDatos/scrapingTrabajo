from read_csv import readSheets
from transformData import transformData
from conectDataBase import conectDataBase
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Obtener credenciales de la base de datos desde variables de entorno
host_dbb = os.getenv('HOST_DBB')
user_dbb = os.getenv('USER_DBB')
pass_dbb = os.getenv('PASSWORD_DBB')
dbb_datalake = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')
dbb_dwheconomico = os.getenv('NAME_DBB_DWH_ECONOMICO')

def load_pbg_data_to_datalake(df_anual, df_trimestral):
    db_connection = conectDataBase(host_dbb, user_dbb, pass_dbb, dbb_datalake)
    """Carga los datos anuales y trimestrales del PBG al datalake."""
    if db_connection.connect_db():
        db_connection.load_data_pbg_anual(df_anual)
        db_connection.load_data_pbg_trimestral(df_trimestral)

def load_filtered_data_to_dwh(df_anual_filtrado):
    db_connection = conectDataBase(host_dbb, user_dbb, pass_dbb, dbb_datalake)
    """Carga los datos anuales filtrados al DWH económico."""
    if db_connection.connect_db():
        db_connection.load_data_pbg_anual(df_anual_filtrado)

def main():
    # Carga de datos PBG
    df = readSheets().readDataPBG()
    transformer = transformData()
    df_anual, df_trimestral = transformer.processData(df)
    
    print("Datos Anuales:")
    print(df_anual)
    print("\nDatos Trimestrales:")
    print(df_trimestral)

    # Cargar datos al datalake
    load_pbg_data_to_datalake(df_anual, df_trimestral)

    #------------------PORCENTAJES DE VARIACIONES------------------#
    # Filtrar datos anuales para 'Valor Agregado Bruto - Precios Constantes 2004' y 'PBG'
    df_anual_filtrado = df_anual[
        (df_anual['Variable'] == 'Valor Agregado Bruto - Precios Constantes 2004') & 
        (df_anual['Actividad'] == 'PBG')
    ]
    print(df_anual_filtrado)

    # Calcular variación interanual
    df_anual_filtrado = transformer.calcular_variacion_anual(df_anual_filtrado)

    print("Datos Anuales con Variación Interanual:")
    print(df_anual_filtrado)

    # Cargar datos filtrados al DWH económico
    load_filtered_data_to_dwh(df_anual_filtrado)

if __name__ == '__main__':
    main()