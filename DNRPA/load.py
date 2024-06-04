from sqlalchemy import create_engine
from pymysql import connect
from pandas import read_sql
from save_data_sheet import readSheets
import pandas as pd

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

        # Asegúrate de que self.conn sea un motor de SQLAlchemy
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")

        # DataFrame de la base de datos
        query = 'SELECT * FROM datalake_economico.dnrpa'
        df_bdd = pd.read_sql(query, engine)

        print("DATAFRAMES IGUALES")
        print(df_bdd.equals(df))

        # Ordena las columnas antes de comparar
        df_bdd = df_bdd[sorted(df_bdd.columns)]
        df = df[sorted(df.columns)]

        # Convertir tipos de datos en df para que coincidan con df_bdd
        df = df.astype({
            'cantidad': 'int64',
            'fecha': 'object',
            'id_provincia_indec': 'int64',
            'id_vehiculo': 'int64'
        })

        # Convertir las columnas 'fecha' a datetime
        df_bdd['fecha'] = pd.to_datetime(df_bdd['fecha'])
        df['fecha'] = pd.to_datetime(df['fecha'])

        # Restablecer los índices
        df_bdd.reset_index(drop=True, inplace=True)
        df.reset_index(drop=True, inplace=True)

        # Eliminar valores faltantes
        df_bdd.fillna(0, inplace=True)
        df.fillna(0, inplace=True)

        # Comparar los DataFrames
        if not df_bdd.equals(df):
            print("Diferencias encontradas entre los DataFrames:")
            query_truncate = 'TRUNCATE dnrpa'
            self.cursor.execute(query_truncate)

            # Cargamos los datos
            df.to_sql(name="dnrpa", con=engine, if_exists='append', index=False)

            print("*****************************************")
            print(" SE HA PRODUCIDO UNA CARGA DE DATOS DE DNRPA ")
            print("*****************************************")

            # Cargamos los datos en el SHEET
            readSheets().cargar_datos(df)
        else:
            print("*****************************************")
            print(" Se realizó una verificación de la base de datos ")
            print(" No existen datos nuevos de DNRPA ")
            print("*****************************************")

        self.close_connections()
                


