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
        print("********************Inicio de la seccion IPC VARIACIONES************************")
        print("\n*****************************************************************************")

        df_bdd = pd.read_sql("SELECT * FROM ipc_variaciones",self.conn)
        tamanio_bdd = len(df_bdd) #--> Tamano de la tabla del emae valores

        tamanio_df = len(df) #Tamano del df extraido.

        if tamanio_df > tamanio_bdd:
            df_tail = df.tail(tamanio_df - tamanio_bdd)
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
            df_tail.to_sql(name="ipc_variaciones", con=engine, if_exists='append', index=False)
            print("*************")
            print(" == ACTUALIZACION == ")
            print(" -- Se ha actualizado la base de datos de EMAE VALORES.")
            print("*************")
        else:
            print("*********")
            print("No existen datos nuevos para cargar de los -- VALORES DE EMAE --")
            print("*********")


       