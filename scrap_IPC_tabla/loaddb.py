import mysql
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd


class conexcionBaseDatos:

    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
    
    def conectar_bdd(self):
        self.conn = mysql.connector.connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    def cargaBaseDatos(self, df):
        print("\n*****************************************************************************")
        print("***********************Inicio de Tabla IPC variaciones************************")
        print("\n*****************************************************************************")
        # Suponiendo que 'df' es tu DataFram
        query_delete = 'TRUNCATE tabla_ipc'
        self.cursor.execute(query_delete)
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
       
    
        df.to_sql(name="tabla_ipc", con=engine, if_exists='replace', index=False)

    def cargaBaseDatos2(self, df):
        print("\n*****************************************************************************")
        print("***********************Inicio de Tabla IPC acumulado************************")
        print("\n*****************************************************************************")
        # Suponiendo que 'df' es tu DataFram
        print(df)
        query_delete = 'TRUNCATE tabla_ipc_acumulados'
        self.cursor.execute(query_delete)
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
       
    
        df.to_sql(name="tabla_ipc_acumulados", con=engine, if_exists='replace', index=False)

     