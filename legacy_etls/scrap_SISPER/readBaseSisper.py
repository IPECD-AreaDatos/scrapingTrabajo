import psycopg2
import pandas as pd

def readBasePostgreSQL(host, user, password, database, query):
    # Establecer la conexión con la base de datos PostgreSQL
    conn = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    
    # Leer la consulta en un DataFrame usando pandas
    df = pd.read_sql_query(query, conn)
    
    # Cerrar la conexión
    conn.close()
    
    return df

