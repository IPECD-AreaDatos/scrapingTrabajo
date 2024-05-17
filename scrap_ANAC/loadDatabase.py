from sqlalchemy import create_engine
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
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="anac", con=engine, if_exists='replace', index=False)
        print("Se realizo la carga a la base de datos anac en datalake_economico")
        
    def read_data_excel(self):
        table_name = 'anac'
        select_row_count_query = f"SELECT corrientes FROM {table_name}"
        
        # Ejecutar la consulta
        self.cursor.execute(select_row_count_query)
        
        # Obtener todos los resultados
        values = self.cursor.fetchall()
        
        # Puedes procesar los resultados según sea necesario
        print("Datos obtenidos de la base de datos:")
        print(values)
        
        # Limpiar y procesar los valores
        clean_values = []
        for value in values:
            value = str(value[0]).replace('.', '')
            if value.isnumeric():
                clean_values.append(int(value))
        
        print("Valores limpios:")
        print(clean_values)
        
        return clean_values
    