import sys
import os
from load_censo_total import load_censo_total
from load_sipa_valores import load_sipa_valores
from load_database import load_database
import pandas as pd

script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, "..", "Credenciales_folder")
sys.path.append(credenciales_dir)
from credenciales_bdd import Credenciales
credenciales_dwh_economico = Credenciales("dwh_economico")
credenciales_ipecd_economico = Credenciales("ipecd_economico")
credenciales_datalake_economico = Credenciales("datalake_economico")

#Fecha de recoleccion de dato
star_date = (2010)
end_year = (2024)

if __name__ == "__main__":
    print("Tabla Censo")
    credenciales_censo = load_censo_total(credenciales_ipecd_economico.host, credenciales_ipecd_economico.user, credenciales_ipecd_economico.password, credenciales_ipecd_economico.database).conectar_bdd()
    df_censo = credenciales_censo.read_censo(star_date, end_year)

    print("Tabla SIPA")
    credenciales_datalake_economico = load_sipa_valores(credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database).conectar_bdd()
    df_sipa = credenciales_datalake_economico.read_sipa(star_date, end_year)
    
    # Armar el DataFrame a partir de las dos tablas
    df_censo['Fecha'] = pd.to_datetime(df_censo['Fecha'])
    df_sipa['fecha'] = pd.to_datetime(df_sipa['fecha'])

    # Asegúrate de que los ID de provincia son del mismo tipo y merge ambas tablas en una sola DataFrame
    combined_df = pd.merge(df_censo, df_sipa, how='inner', left_on=['ID_Provincia', 'Fecha'], right_on=['id_provincia', 'fecha'])
    print(combined_df)
    exit()
    # Calcular la tasa de empleo por cada mil habitantes
    combined_df['puestos_cada_mil_empleados'] = ((combined_df['cantidad_sin_estacionalidad'] * 1000) / combined_df['Poblacion']) * 1000

    # Seleccionar solo las columnas deseadas para el DataFrame final
    combined_df = combined_df[['fecha', 'id_provincia', 'puestos_cada_mil_empleados']]

    # Visualizar el resultado
    print(combined_df)

    credenciales_dwh_economico = load_database(credenciales_dwh_economico.host, credenciales_dwh_economico.user, credenciales_dwh_economico.password, credenciales_dwh_economico.database).conectar_bdd()
    credenciales_dwh_economico.load_data(combined_df)