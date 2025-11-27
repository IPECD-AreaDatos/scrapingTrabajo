import sys
import os
from load_censo_total import load_censo_total
from load_sipa_valores import load_sipa_valores
from load_database import load_database
import pandas as pd
# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_dwh = (os.getenv('NAME_DBB_DWH_ECONOMICO'))
dbb_dtl = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))
dbb_dtlsocio = (os.getenv('NAME_DBB_DATALAKE_SOCIO'))
host_dbb,user_dbb,pass_dbb,dbb_dwh
host_dbb,user_dbb,pass_dbb,dbb_dtl

#Fecha de recoleccion de dato
star_date = (2010)
end_year = (2024)

if __name__ == "__main__":
    print("Tabla Censo")
    credenciales_censo = load_censo_total(host_dbb,user_dbb,pass_dbb,dbb_dtlsocio).conectar_bdd()
    df_censo = credenciales_censo.read_censo(star_date, end_year)

    print("Tabla SIPA")
    credenciales_datalake_economico = load_sipa_valores(host_dbb,user_dbb,pass_dbb,dbb_dtl).conectar_bdd()
    df_sipa = credenciales_datalake_economico.read_sipa(star_date, end_year)
    
    # Armar el DataFrame a partir de las dos tablas
    df_censo['fecha'] = pd.to_datetime(df_censo['fecha'])
    df_sipa['fecha'] = pd.to_datetime(df_sipa['fecha'])

    # Asegúrate de que los ID de provincia son del mismo tipo y merge ambas tablas en una sola DataFrame
    combined_df = pd.merge(df_censo, df_sipa, how='inner', left_on=['id_provincia', 'fecha'], right_on=['id_provincia', 'fecha'])
    
    # Calcular la tasa de empleo por cada mil habitantes
    combined_df['puestos_cada_mil_empleados'] = ((combined_df['cantidad_sin_estacionalidad'] * 1000) / combined_df['poblacion']) * 1000

    # Seleccionar solo las columnas deseadas para el DataFrame final
    combined_df = combined_df[['fecha', 'id_provincia', 'puestos_cada_mil_empleados']]

    # Visualizar el resultado

    credenciales_dwh_economico = load_database(host_dbb,user_dbb,pass_dbb,dbb_dwh).conectar_bdd()
    credenciales_dwh_economico.load_data(combined_df)