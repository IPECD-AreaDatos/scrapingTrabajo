import datetime
import time
import xlrd
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector



class LoadXLSDregiones:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
    
    def conectar_bdd(self):
        self.conn = mysql.connector.connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()
        return self

    def crear_tabla(self, ruta):
        # conexion con la base de datos
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")

        # df de ipc con las subdivisiones
        tabla_subdivision = pd.read_sql_query("SELECT * FROM ipc_subdivision", con=engine) 

        # Crear una columna de código combinando las columnas 'categoria', 'division' y 'subdivicion'
        tabla_subdivision['codigo'] = tabla_subdivision.apply(lambda row: [int(row['id_categoria']), int(row['id_division']), int(row['id_subdivision'])],axis=1)

        # Crear un diccionario de mapeo de nombre a código (lista de códigos)
        nombre_a_codigo = dict(zip(tabla_subdivision['nombre'], tabla_subdivision['codigo']))

        print("Diccionario de mapeo:")
        print(nombre_a_codigo)

        # df del excel con valores
        df = pd.read_excel(ruta, sheet_name='Índices aperturas', skiprows=5, nrows=46)
        df = df.drop(0)

        # cambiamos la columna de categoria por su codigo
        df['Región GBA'] = df['Región GBA'].map(nombre_a_codigo)
        # Unpivot the DataFrame to long format
        #df = df.melt(id_vars=['Región GBA'], var_name='fecha', value_name='valor')
        
        # Mapea la columna de categorías a los códigos correspondientes
        #df[['id_categoria', 'id_division', 'id_subdivision']] = df['Región GBA'].apply(lambda x: pd.Series(nombre_a_codigo.get(x, [None, None, None])))


        print(df)


        # Crear las columnas id_categoria, id_division, id_subdivision a partir de la columna 'Región GBA'
        df[['id_categoria', 'id_division', 'id_subdivision']] = pd.DataFrame(df['Región GBA'].tolist(), index=df.index)

        # Eliminar la columna 'Región GBA' ya que hemos descompuesto su contenido
        df = df.drop(columns=['Región GBA'])

        # Usamos pd.melt para "dar vuelta" el DataFrame
        df_melted = pd.melt(df, id_vars=['id_categoria', 'id_division', 'id_subdivision'], var_name='fecha', value_name='valor')
        df_melted = df_melted[['fecha', 'id_categoria', 'id_division', 'id_subdivision', 'valor']]
        df_melted['fecha'] = pd.to_datetime(df_melted['fecha'], errors='coerce')

        new_column = pd.Series(2, name='id_region')
        df_melted.insert(loc=1, column='id_region', value=new_column)
        df_melted['id_region']=2
        print(df_melted)

        return df