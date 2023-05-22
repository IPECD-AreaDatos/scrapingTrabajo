import mysql.connector
import numpy as np
import pandas as pd

#pip install pandas
#pip install mysql-connector-python
#pip install openpyxls

# Establecer la conexión a la base de datos
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='Estadistica123',
    database='prueba1'
)

# Ruta del archivo de csvread_csv
csv_file = 'C:\\Users\\Usuario\\Documents\\Matias\\Codigo\\Actualizado.csv'

# Nombre de la tabla en MySQL
table_name = 'Puestos_Trabajo_Asalariado'

# Leer el archivo de csvread_csv
df = pd.read_csv(csv_file) #Data Frame//Libreria de Panda
df = df.replace({np.nan: None})

# Obtener los nombres y tipos de datos de las columnas
column_names = list(df.columns)
column_types = df.dtypes.to_dict()

# Crear la tabla en MySQL
create_table_query = f"CREATE TABLE {table_name} ("
for column_name in column_names:
    data_type = column_types[column_name]
    if data_type == 'int64':
        create_table_query += f"{column_name} INT, "
    elif data_type == 'float64':
        create_table_query += f"{column_name} FLOAT, "
    elif data_type == 'datetime64[ns]':
        create_table_query += f"{column_name} DATETIME, "
    else:
        create_table_query += f"{column_name} VARCHAR(255), "
create_table_query = create_table_query.rstrip(', ') + ")"
#create_table_query += "PRIMARY KEY (codigo_departamento_indec))" #Para establecer una PK
conn.cursor().execute(create_table_query)

# Insertar los datos del archivo de csvread_csv en la tabla
insert_query = f"INSERT INTO {table_name} VALUES ("
for index, row in df.iterrows():
    values = ', '.join(["%s" for _ in range(len(row))])
    data_tuple = tuple(row.values)
    conn.cursor().execute(insert_query + values + ")", data_tuple)
conn.commit()

# Cerrar la conexión a la base de datos
conn.close()