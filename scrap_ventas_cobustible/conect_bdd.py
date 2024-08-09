from pymysql import connect
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
        self.conn = connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    #Objetivo: obtener tamanos de base de datos, y del df, posteriormente se usa para verificar la carga
    def verificar_carga(self, df):

        #Obtencion del tamano de los datos de la bdd
        select_row_count_query = f"SELECT COUNT(*) FROM combustible"
        self.cursor.execute(select_row_count_query)
        tamano_bdd = self.cursor.fetchone()[0]   

        #Obtenemos tamano del DF a cargar
        tamano_df = len(df) 

        return tamano_bdd,tamano_df
    
    #Objetivo: realizar efectivamente la carga a la BDD
    def cargaBaseDatos(self, df):
        print("\n*****************************************************************************")
        print("*********************Inicio de la seccion venta Combustible**********************")
        print("\n*****************************************************************************")
        
        #Obtencion de cantidades de datos
        tamano_bdd, tamano_df = self.verificar_carga(df)
        print(tamano_bdd,tamano_df)
        
        if tamano_df > tamano_bdd:
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")

            df_tail = df.tail(tamano_df - tamano_bdd)
            df_tail.to_sql(name='combustible', con=engine, if_exists='append', index=False)

            print("*************")
            print(f" == ACTUALIZACION == Nuevos datos en la base de combustibles")
            print("*************")
            return True
        else:
            print("*********")
            print("No existen datos nuevos de combustible")
            print("*********")
            return False
    
    
    def main(self, df):
        self.conectar_bdd()

        bandera = self.cargaBaseDatos(df)

        #Cerramos conexiones
        self.conn.commit()
        self.conn.close()
        self.cursor.close()

        #Condicion final
        bandera = bandera
        return bandera
