import mysql.connector
import pandas as pd
from datetime import datetime
import calendar
from email.message import EmailMessage
import ssl
import smtplib
from pandas import isna

class connect_db:
    def connect(self, df, host, user, password, database):
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'eph_tasas'
        delete_query = f"TRUNCATE {database}.{table_name}"
        query_cantidad_datos = f'SELECT COUNT(*) FROM {table_name}'
        query_insertar_datos = f"INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s, %s, %s)"
        
        #Obtencion de tamaño del DF sacado del EXCEL
        cant_filas_df = len(df)
        #Obtencion de cantidad de filas de 
        cursor.execute(query_cantidad_datos)
        row_count_before = cursor.fetchone()[0]

        if (cant_filas_df > row_count_before):
            #Eliminamos tabla
            cursor.execute(delete_query)

            #Iteramos el dataframe, y vamos cargando fila por fila
            for index,valor in df.iterrows():
                values = (valor['Aglomerado'], valor['Año'], valor['Fecha'], valor['Trimestre'], valor['Tasa de Actividad'], valor['Tasa de Empleo'], valor['Tasa de desocupación'])
                # Convertir valores NaN a None --> Lo hacemos porque los valores 'nan' no son reconocidos por MYSQL
                values = [None if isna(v) else v for v in values]
                
                #Insercion de dato fila por fila
                cursor.execute(query_insertar_datos,values)
            conn.commit()
            cursor.close()
            conn.close()
        else:
            print(f"NO HAY CAMBIOS EN LOS DATOS DE {table_name}")

