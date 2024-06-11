from sqlalchemy import create_engine, text
import mysql
import mysql.connector
import pandas as pd


class load_database:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def conectar_bdd(self):
        self.conn = mysql.connector.connect(
            host=self.host, user=self.user, password=self.password, database=self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    def load_data(self, df):
        # Crear la conexión al motor de la base de datos
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        
        # Cargar el DataFrame a la base de datos
        df.to_sql(name="anac", con=engine, if_exists='replace', index=False)

    def read_data_excel(self):
        table_name = 'anac'
        select_row_count_query = f"SELECT corrientes FROM {table_name}"
        self.cursor.execute(select_row_count_query)
        
        values = self.cursor.fetchall()
        
        print("Datos obtenidos de la base de datos:")
        print(values)
        
        clean_values = []
        for value in values:
            value_str = str(value[0]).strip()
            value_str = value_str.replace(',', '.')
            try:
                clean_value = float(value_str)
                clean_values.append(clean_value)
            except ValueError:
                pass
        
        print("Valores limpios:")
        print(clean_values)
        return clean_values
        