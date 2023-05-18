import mysql.connector
import numpy as np
import pandas as pd
import time

class LoadCSVData:
    def loadInDataBase(self, file_path):
        start_time = time.time()
        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='Estadistica123',
            database='prueba1'
        )

        # Nombre de la tabla en MySQL
        table_name = 'puestos_trabajo_asalariado'

        # Leer el archivo de csvread_csv
        df = pd.read_csv(file_path) #Data Frame//Libreria de Panda
        df = df.replace({np.nan: None})

        # Obtener los nombres y tipos de datos de las columnas
        column_names = list(df.columns)
        column_types = df.dtypes.to_dict()

        print("ANTES DEL FOR")
        insert_query = f"INSERT INTO {table_name} VALUES ("
        for index, row in df.iterrows():
            print("en el for:")
            values = ', '.join(["%s" for _ in range(len(row))])
            data_tuple = tuple(row.values)
            conn.cursor().execute(insert_query + values + ")", data_tuple)
            print("values")
            print(data_tuple)
        conn.commit()
        
        print("GUARDO!!!!!")
        end_time = time.time()
        duration = end_time - start_time
        print(f"Tiempo de ejecución: {duration} segundos")
        
        # Cerrar la conexión a la base de datos
        conn.close()
    