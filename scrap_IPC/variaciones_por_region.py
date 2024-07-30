import datetime
import time
import xlrd
import pandas as pd
from sqlalchemy import create_engine
import mysql.connector
import pymysql 



class LoadXLSDregiones:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
    
    def conectar_bdd(self):
        try:
            self.conn = mysql.connector.connect(
                host=self.host, user=self.user, password=self.password, database=self.database
            )
            self.cursor = self.conn.cursor()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        return self

    def armado_dfs(self, ruta):
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
        df_original = pd.read_excel(ruta, sheet_name='Variación mensual aperturas', skiprows=5, nrows=295)
        df_original['Región GBA'] = df_original['Región GBA'].map(nombre_a_codigo)

        #print(df_original.head(96))
        #print(df_original)
        # Asegurarse de que el DataFrame no sea más pequeño que la cantidad de filas necesarias
        primer_tamano = 50
        tamano_resto= 49
        if len(df_original) < primer_tamano + tamano_resto:
            raise ValueError("El DataFrame es demasiado pequeño para dividir con los tamaños proporcionados.")
        
        # Obtener el primer DataFrame
        df1 = df_original.iloc[:primer_tamano].copy()
        print("DataFrame 1:")
        print(df1)        
        # Guardar el primer DataFrame en una variable
        dfs = [df1]
        # Obtener el resto de los DataFrames
        resto_df = df_original.iloc[primer_tamano:].copy()
        contador = 2
        while len(resto_df) > 0:
            df_nuevo = resto_df.iloc[:tamano_resto].copy()
            
            # Eliminar la fila en la posición 1 (índice 0) de los DataFrames después del primero
            if contador >= 2:
                df_nuevo = df_nuevo.iloc[1:].copy()

            dfs.append(df_nuevo)
            print(f"DataFrame {contador}:")
            #print(df_nuevo)
            
            # Guardar cada DataFrame en una nueva variable
            #globals()[f'df_{contador}'] = df_nuevo

            resto_df = resto_df.iloc[tamano_resto:].copy()
            contador += 1


        region = 2
        dfs_editados = []
        for df in dfs:
            df = df.copy()
            # Asegúrate de que 'Región GBA' es una lista
            df['Región GBA'] = df['Región GBA'].apply(lambda x: x if isinstance(x, list) else [None, None, None])

            # Descomponer 'Región GBA' en tres columnas utilizando .loc
            df.loc[:, ['id_categoria', 'id_division', 'id_subdivision']] = pd.DataFrame(df['Región GBA'].tolist(), index=df.index)

            # Eliminar la columna 'Región GBA'
            df = df.drop(columns=['Región GBA'])

            # Usar pd.melt para "dar vuelta" el DataFrame
            df_melted = pd.melt(df, id_vars=['id_categoria', 'id_division', 'id_subdivision'], var_name='fecha', value_name='valor')
            df_melted = df_melted[['fecha', 'id_categoria', 'id_division', 'id_subdivision', 'valor']]
            df_melted['fecha'] = pd.to_datetime(df_melted['fecha'], errors='coerce')

            df_melted['id_region'] = region
            df_melted = df_melted[['fecha', 'id_region', 'id_categoria', 'id_division', 'id_subdivision', 'valor']]

            dfs_editados.append(df_melted)
            region += 1
            print("-------------------------------")
            print(df)
            
        return dfs_editados


    def df_individual(self, region, df):
        
        df = df.iloc[2:]

        # Asegúrate de que 'Región GBA' es una lista
        df['Región GBA'] = df['Región GBA'].apply(lambda x: x if isinstance(x, list) else [None, None, None])

        # Descomponer 'Región GBA' en tres columnas utilizando .loc
        df.loc[:, ['id_categoria', 'id_division', 'id_subdivision']] = pd.DataFrame(df['Región GBA'].tolist(), index=df.index)

        # Eliminar la columna 'Región GBA' ya que hemos descompuesto su contenido
        df = df.drop(columns=['Región GBA'])

        # Usamos pd.melt para "dar vuelta" el DataFrame
        df_melted = pd.melt(df, id_vars=['id_categoria', 'id_division', 'id_subdivision'], var_name='fecha', value_name='valor')
        df_melted = df_melted[['fecha', 'id_categoria', 'id_division', 'id_subdivision', 'valor']]
        df_melted['fecha'] = pd.to_datetime(df_melted['fecha'], errors='coerce')

        df_melted['id_region'] = region
        df_melted = df_melted[['fecha', 'id_region', 'id_categoria', 'id_division', 'id_subdivision', 'valor']]

        return df_melted
    
    def procesar_dfs(self, ruta):
        # Obtener los DataFrames divididos
        dfs = self.armado_dfs(ruta)

        # Procesar cada DataFrame individualmente
        region = 2
        dfs_editados = []
        for df in dfs:  
            df_editado = self.df_individual(region, df)
            dfs_editados.append(df_editado)
            region += 1

        return dfs_editados