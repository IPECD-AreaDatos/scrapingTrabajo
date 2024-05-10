from sqlalchemy import create_engine
from pymysql import connect
from pandas import read_sql
from save_data_sheet import readSheets

class conexionBaseDatos:

    #Inicializacion de variables en la clase
    def __init__(self, host, user, password, database):

        self.host = host
        self.user = user
        self.password = password 
        self.database = database
        self.cursor = None
        self.conn = None


    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE A LAS CONEXIONES ==== #
    # =========================================================================================== #        

    #Objetivo: conectar a la base de datos
    def connect_db(self):

            self.conn = connect(
                host=self.host, user=self.user, password=self.password, database=self.database
            )
            self.cursor = self.conn.cursor()


    #Objetivo: guardar los ultimos cambios hechos y cerrar las conexiones
    def close_connections(self):
        self.conn.commit()
        self.conn.close()
        self.cursor.close()


    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE A LAS CARGAS ==== #
    # =========================================================================================== #          

    #En este caso vamos a cargar el DATALAKE ECONOMICO

    def cargar_datalake(self,df):

        self.connect_db()
        
        #DF de la base de datos
        query = 'select * from datalake_economico.dnrpa'
        df_bdd =read_sql(query,self.conn)

        
        print("Datos extraidos")
        print(df)
        print("BASE DE DATOS ")
        print(df_bdd)

        if not(df_bdd.equals(df)): 

            query_truncate = 'TRUNCATE dnrpa'
            self.cursor.execute(query_truncate)

            #Conectamos a la BDD
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
            df.to_sql(name="dnrpa", con=engine, if_exists='append', index=False)

            #Cargamos los datos en el SHEET (https://docs.google.com/spreadsheets/d/1L_EzJNED7MdmXw_rarjhhX8DpL7HtaKpJoRwyxhxHGI/edit#gid=0)
            readSheets().cargar_datos(df)






