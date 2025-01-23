from sqlalchemy import create_engine
from pymysql import connect
from save_data_sheet import readSheets
import pandas as pd
from datetime import datetime, date, timedelta
import sys


class conexionBaseDatosLast:

    def __init__(self, host, user, password, database):

        self.host = host
        self.user = user
        self.password = password 
        self.database = database
        self.cursor = None
        self.conn = None

    def connect_db(self):
            print(f"Intentando conectar a: host={self.host}, user={self.user}, password={self.password}, database={self.database}")
            self.conn = connect(
                host=self.host, user=self.user, password=self.password, database=self.database
            )
            self.cursor = self.conn.cursor()
            print("conec establecida")

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

        df = df.sort_values(by='fecha', ascending=True)

        ultima_fila = df.iloc[0]

        ultimo_anio = ultima_fila['fecha'].year

        #print(f"Año de la última fila agregada: {ultimo_anio}")        

        query_count = f'SELECT COUNT(*) FROM datalake_economico.dnrpa WHERE YEAR(fecha) = {ultimo_anio}'
        self.cursor.execute(query_count)
        cantidad_tabla = self.cursor.fetchone()[0]

        # Obtener la cantidad de filas en el DataFrame df para el año 2024
        cantidad_df = len(df[df['fecha'].dt.year == ultimo_anio])

        print(f"Cantidad de filas en la tabla dnrpa para 2024: {cantidad_tabla}")
        print(f"Cantidad de filas en el DataFrame df para 2024: {cantidad_df}")

        if cantidad_tabla == cantidad_df:
            print("*****************************************")
            print(" Se realizó una verificación de la base de datos ")
            print(f" No existen datos nuevos de DNRPA para {ultimo_anio} ")
            print("*****************************************")
        else:
            print("*****************************************")
            print(f" Diferencias encontradas entre los datos de {ultimo_anio}. Truncando la tabla dnrpa... ")

            # Truncar la tabla dnrpa para el año 2024
            query_truncate = f'DELETE FROM datalake_economico.dnrpa WHERE YEAR(fecha) = {ultimo_anio}'
            self.cursor.execute(query_truncate) #Ejecutamos el delete
            self.conn.commit() #Confirmamos DELETE de datos

            # Cargar los datos del DataFrame df para el año 2024 a la tabla dnrpa
            df_2024 = df[df['fecha'].dt.year == ultimo_anio]
            print("datos a cargar")
            print(df_2024)
            df_2024.to_sql(name="dnrpa", con=engine, if_exists='append', index=False)

            print("*****************************************")
            print(f" SE HA PRODUCIDO UNA CARGA DE DATOS DE DNRPA para {ultimo_anio} ")
            print("*****************************************")


            #Carga de datos en google sheets
            readSheets(self.host, self.user, self.password, self.database).main()


        self.close_connections()


