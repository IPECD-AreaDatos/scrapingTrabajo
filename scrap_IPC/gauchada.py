from sqlalchemy import create_engine
import pymysql
import pandas as pd
import sys

host = '54.94.131.196'
user = 'estadistica'
password = 'Estadistica2024!!'
database = 'datalake_economico'

conn = pymysql.connect(
    host=host, user=user, password=password, database=database
)
cursor = conn.cursor()

query = 'SELECT * FROM datalake_economico.ipc_valores'
df = pd.read_sql(query,conn)

# Definir el diccionario de mapeo de nombres de columnas
nuevos_nombres = {
            'fecha': 'Fecha',
            'id_region': 'ID_Region',
            'id_categoria': 'ID_Categoria',
            'id_division': 'ID_Division',
            'id_subdivision': 'ID_Subdivision',
            'valor': 'Valor'
        }

# Renombrar las columnas utilizando el diccionario de mapeo
df = df.rename(columns=nuevos_nombres)


query_truncate = "TRUNCATE ipecd_economico.ipc_region"
cursor.execute(query_truncate)

#Cargamos los datos usando una query y el conector. Ejecutamos las consultas
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{3306}/ipecd_economico")
df.to_sql(name="ipc_region", con=engine, if_exists='append', index=False)


print("DATOS CARGADOS")
