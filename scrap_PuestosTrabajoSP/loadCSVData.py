import mysql.connector
import numpy as np
import pandas as pd
import time
import os

class LoadCSVData:
    def loadInDataBase(self, host, user, password, database):
        #Se toma el tiempo de comienzo
        start_time = time.time()
        
        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )

        # Nombre de la tabla en MySQL
        table_name = 'puestos_trabajo_asalariado'
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        # Construir la ruta de la carpeta "files" dentro del directorio actual
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_name = "trabajoSectorPrivado.csv"
        # Construir la ruta completa del archivo CSV dentro de la carpeta "files"
        file_path = os.path.join(ruta_carpeta_files, file_name)
                
        # Leer el archivo de csv y hacer transformaciones
        df = pd.read_csv(file_path)  # Leer el archivo CSV y crear el DataFrame
        df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

        print("columnas -- ", df.columns)

        # Aplicar strip() al nombre de la columna antes de acceder a ella
        column_name = ' puestos '  # Nombre de la columna con espacios en blanco
        column_name_stripped = column_name.strip()  # Eliminar espacios en blanco

        # Verificar si la columna existe en el DataFrame
        if column_name_stripped in df.columns:
            # Realizar transformaciones en el DataFrame utilizando el nombre de columna sin espacios
            df.loc[df[column_name_stripped] < 0, column_name_stripped] = 0 #Los datos <0 se reempalazan a 0
        else:
            print(f"La columna '{column_name_stripped}' no existe en el DataFrame.")

        # Obtener los nombres y tipos de datos de las columnas
        column_names = list(df.columns)
        column_types = df.dtypes.to_dict()

        #Representa la consulta SQL de inserción en la tabla de la base de datos. 
        insert_query = f"INSERT INTO {table_name} VALUES ("
        for index, row in df.iterrows(): #Se itera sobre las filas del DataFrame
            print("en el for:")
            #Se crea una cadena values que contiene marcadores de posición %s para cada valor en una fila.
            values = ', '.join(["%s" for _ in range(len(row))])
            #Se convierte la fila en una tupla de valores, que se utilizará como argumento en la consulta de inserción.
            data_tuple = tuple(row.values)
            #Se ejecuta la consulta de inserción utilizando el objeto cursor//Se concatenan la consulta insert_query, los valores values y un paréntesis de cierre
            #La tupla data_tuple se pasa como argumento para proporcionar los valores a insertar en la tabla.
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