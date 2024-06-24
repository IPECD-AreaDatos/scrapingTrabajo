import mysql
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd


class loadDataBase:

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
        print("***********************Inicio de IPC************************")
        print("\n*****************************************************************************")

        #engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
       
        #df.to_sql(name="ipc_online", con=engine, if_exists='append', index=False)

        # Establecer conexión a la base de datos
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")

        df_existente = pd.read_sql("SELECT fecha FROM ipc_online", con=engine)

        # Verificar si alguna fecha nueva ya existe en la base de datos
        nueva_fila = df[~df['fecha'].isin(df_existente['fecha'])]

        if not nueva_fila.empty:
            nueva_fila.to_sql(name="ipc_online", con=engine, if_exists='append', index=False)
            print("Se cargaron datos nuevos a la base de datos.")
        else:
            print("No hay datos nuevos para cargar en la base de datos.")

        self.conn.close()
        
        
