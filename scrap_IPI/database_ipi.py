import mysql.connector
from datetime import datetime, date
import pandas as pd
from dateutil.relativedelta import relativedelta
from correo_ipi_nacion import Correo_ipi_nacion

class Database_ipi:

    def __init__(self):
        

        self.conn = None
        self.cursor = None

    def conectar_bdd(self, host, user, password, database):

        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.conn.cursor()


    def cargar_datos(self, host, user, password, database, df):

        self.conectar_bdd(host, user, password, database)

        table_name= 'ipi'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        self.cursor.execute(select_row_count_query)
        filas_BD = self.cursor.fetchone()[0]

        print("Tamaño base de datos:", filas_BD)
        print("Tamaño del dataframe construido",len(df))


        #Vemos las longitudes de los datos guardados, y del archivo descargado. Para cargar solo  lo nuevo
        longitud_df = len(df)
        if filas_BD != len(df):
            df_datos_nuevos = df.tail(longitud_df - filas_BD)
            print(df_datos_nuevos)

            for fecha,var_ipi,var_interanual_alimentos,var_interanual_textil,var_interanual_maderas,var_interanual_sustancias,var_interanual_MinNoMetalicos,var_interanual_metales in zip(df_datos_nuevos['fecha'],df_datos_nuevos['var_IPI'],df_datos_nuevos['var_interanual_alimentos'],df_datos_nuevos['var_interanual_textil'],df_datos_nuevos['var_interanual_sustancias'],df_datos_nuevos['var_interanual_maderas'],df_datos_nuevos['var_interanual_MinNoMetalicos'],df_datos_nuevos['var_interanual_metales']):
                

                print(fecha)
                sql_insert = "INSERT INTO ipi (fecha,var_IPI, var_interanual_alimentos, var_interanual_textil,var_interanual_sustancias, var_interanual_maderas, var_interanual_minnoMetalicos, var_interanual_metales) VALUES (%s, %s, %s,%s, %s, %s,%s, %s)"
                values = (fecha,var_ipi,var_interanual_alimentos,var_interanual_textil,var_interanual_maderas,var_interanual_sustancias,var_interanual_MinNoMetalicos,var_interanual_metales)
                self.cursor.execute(sql_insert,values)
                print(sql_insert)
                # Ejecutar la sentencia SQL de inserción

            self.conn.commit()

            #SECCION DE ENVIO DE CORREO     
            instancia_correo = Correo_ipi_nacion()
            instancia_correo.connect(host, user, password,database)
            instancia_correo.construccion_correo()

    


    
