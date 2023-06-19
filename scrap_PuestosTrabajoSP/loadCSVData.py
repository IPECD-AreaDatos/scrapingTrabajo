import mysql.connector
import numpy as np
import pandas as pd
import time

class LoadCSVData:
    def loadInDataBase(self, file_path):
        #Se toma el tiempo de comienzo
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
        df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

        # Verificar si la tabla ya contiene registros
        select_query = f"SELECT fecha, codigo_departamento_indec, id_provincia_indec, clae2, puestos FROM {table_name}"
        existing_records = pd.read_sql(select_query, conn)

        # Filtrar los registros del archivo CSV que no se encuentran en la tabla
        df_to_insert = df[~df.set_index(existing_records.columns).index.isin(existing_records.set_index(existing_records.columns).index)]

        # Aplicar strip() al nombre de la columna antes de acceder a ella
        column_name = ' puestos '  # Nombre de la columna con espacios en blanco
        column_name_stripped = column_name.strip()  # Eliminar espacios en blanco
        # Verificar si la columna existe en el DataFrame
        if column_name_stripped in df.columns:
            # Realizar transformaciones en el DataFrame utilizando el nombre de columna sin espacios
            df_to_insert.loc[df_to_insert[column_name_stripped] < 0, column_name_stripped] = 0
        else:
            print(f"La columna '{column_name_stripped}' no existe en el DataFrame.")

        # Obtener los nombres y tipos de datos de las columnas
        column_names = list(df.columns)
        column_types = df.dtypes.to_dict()

        #Representa la consulta SQL de inserción en la tabla de la base de datos. 
        insert_query = f"INSERT INTO {table_name} VALUES ("
        for index, row in df_to_insert.iterrows():
            # Se crea una cadena values que contiene marcadores de posición %s para cada valor en una fila.
            values = ', '.join(["%s" for _ in range(len(row))])
            # Se convierte la fila en una tupla de valores, que se utilizará como argumento en la consulta de inserción.
            data_tuple = tuple(row.values)
            # Se ejecuta la consulta de inserción utilizando el objeto cursor
            conn.cursor().execute(insert_query + values + ")", data_tuple)
            print("values")
            print(data_tuple)

        conn.commit()
        
        print("GUARDO!!!!!")
        #Se toma el tiempo de finalizacion y se calcula
        end_time = time.time()
        duration = end_time - start_time
        print(f"Tiempo de ejecución: {duration} segundos")
        
        # Cerrar la conexión a la base de datos
        conn.close()
    