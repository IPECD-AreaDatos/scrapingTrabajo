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
        
        # Leer el archivo de csv y hacer transformaciones
        df = pd.read_csv(file_path)  # Leer el archivo CSV y crear el DataFrame
        df = df.replace({np.nan: None})  # Reemplazar los valores NaN por None

        print("columnas -- ", df.columns)

        # Aplicar strip() al nombre de la columna antes de acceder a ella
        column_name = ' puestos '  # Nombre de la columna con espacios en blanco
        column_name_stripped = column_name.strip()  # Eliminar espacios en blanco

        # Verificar si la columna existe en el DataFrame
        if column_name_stripped in df.columns:
            # Realizar transformaciones en el DataFrame utilizando el nombre de columna sin espacios
            df.loc[df[column_name_stripped] < 0, column_name_stripped] = 0
        else:
            print(f"La columna '{column_name_stripped}' no existe en el DataFrame.")

        # Obtener los nombres y tipos de datos de las columnas
        column_names = list(df.columns)
        column_types = df.dtypes.to_dict()
        
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
    