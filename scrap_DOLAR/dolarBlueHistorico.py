import pandas as pd
from sqlalchemy import create_engine
import os

class dolarBlueHistorico:
    def __init__(self, user, password, host, database, table_name):
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.table_name = table_name

    def load_xlsx_to_mysql(self):
        try:
            directorio_actual = os.path.dirname(os.path.abspath(__file__))
            ruta_carpeta_files = os.path.join(directorio_actual, 'files')
            file_name = "dolar_blue.xlsx"
            file_path = os.path.join(ruta_carpeta_files, file_name)

            # Leer el archivo .xlsx
            df = pd.read_excel(file_path)
            # Convertir el formato de la fecha
            df['fecha'] = pd.to_datetime(df['fecha'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
            # Conectar a la base de datos MySQL
            engine = create_engine(f'mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}')

            # Escribir el DataFrame en una tabla de MySQL
            df.to_sql(self.table_name, con=engine, if_exists='append', index=False)

            print("Datos cargados exitosamente en la base de datos.")
        except Exception as e:
            print(f"Error al cargar los datos: {e}")
