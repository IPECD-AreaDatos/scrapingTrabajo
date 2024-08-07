"""
Archivo destinado a construir el DATAFRAME  que contendra:

* Valores de la serie original. Cada dato dividido por REGION,CATEGORIA,DIVISION,SUBDIVISION
"""
import pandas as pd
from sqlalchemy import create_engine
import os

class TransformRegionesValores:

    #Inicializacion de atributos
    def __init__(self, host, user, password, database):

        self.engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{3306}/{database}")



    #Objetivo: construir la ruta de acceso a la carpeta FILES
    def construccion_rutas(self):

        #Carpeta scrap_IPC
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')

        #Direcciones de los Excels
        path_ipc_apertura = os.path.join(ruta_carpeta_files, 'sh_ipc_aperturas.xls') #Dato por region
        path_ipc_mes_ano = os.path.join(ruta_carpeta_files, 'sh_ipc_mes_ano.xls') #Dato nacional

        return path_ipc_apertura, path_ipc_mes_ano


    def armado_dfs(self):
        
        
        #Obtencion de rutas
        path_ipc_apertura, path_ipc_mes_ano = self.construccion_rutas()

        #=== CONSTRUCCION DE TABLA DE MAPEO - Se utiliza las subdivisiones como refencia

        tabla_subdivision = pd.read_sql_query("SELECT * FROM ipc_subdivision", con=self.engine) 

        # Crear una columna 'codigo' combinando las columnas 'categoria', 'division' y 'subdivicion' creando una lista con 3 valores enteros
        tabla_subdivision['codigo'] = tabla_subdivision.apply(lambda row: [int(row['id_categoria']), int(row['id_division']), int(row['id_subdivision'])],axis=1)

        # Crear un diccionario de mapeo entre 'nombre' y la lista de 'codigo' que le corresponde
        nombre_a_codigo = dict(zip(tabla_subdivision['nombre'], tabla_subdivision['codigo']))

        #=== LECUTURA DEL EXCEL DIVIDO POR REGION


        df_original = pd.read_excel(path_ipc_apertura, sheet_name=2, skiprows=5, nrows=295)
        df_original.rename(columns={'Región GBA': 'claves_listas'}, inplace=True)


        #=== LECUTURA DEL EXCEL NACIONAL

        # Cargamos el df de nacion unicamente, cambiamos la columna de categorias por el mismo nombre 'claves_listas' y hacemos el mapeo, agregamos el df primero en la lista de dfs
        df_nacion = self.df_nacion(path_ipc_mes_ano)
        df_nacion.rename(columns={'Total nacional': 'claves_listas'}, inplace=True)
        df_nacion['claves_listas'] = df_nacion['claves_listas'].map(nombre_a_codigo)
        dfs = [df_nacion]

        # Mapea la columna donde estaban las categorias con el diccionario, reeplazando el string por una lista de tres valores
        df_original['claves_listas'] = df_original['claves_listas'].map(nombre_a_codigo)

        # Tamaños necesarios para dividir el df, ya que tienen diferente cantidad de filas
        primer_tamano = 49
        tamano_resto= 48

        # Obtener el primer DataFrame: GBA y lo guardamos en una lista de dfs
        df1 = df_original.iloc[:primer_tamano]
        dfs.append(df1)


        # Obtener el DataFrame sin la primer region, osea de gba en adelante
        resto_df = df_original.iloc[primer_tamano:]
        contador = 2

        # Mientras el df tenga filas crea un nuevo df con el tamaño establecido, lo agrega a la lista 
        while len(resto_df) > 0:
            df_nuevo = resto_df.iloc[:tamano_resto]
            
            # Eliminar la primera fila en los dfs despues del primero, para dejarlos iguales
            if contador >= 2:
                df_nuevo = df_nuevo.iloc[1:]

            dfs.append(df_nuevo)
            resto_df = resto_df.iloc[tamano_resto:]
            contador += 1
        
        # Eliminar el último DataFrame de la lista
        if dfs:
            dfs.pop()

        region = 1 # Contador que representa la region
        dfs_editados = []

        # Busca editar cada uno de los df que estan en la lista de dfs
        for df in dfs:
            df['claves_listas'] = df['claves_listas'].apply(lambda x: x if isinstance(x, list) else [None, None, None])

            # Descomponer 'Región GBA' que tiene una lista de 3 valores en tres columnas
            df[['id_categoria', 'id_division', 'id_subdivision']] = pd.DataFrame(df['claves_listas'].tolist(), index=df.index)

            df = df.drop(columns=['claves_listas'])

            # Usar pd.melt para dar vuelta el DataFrame en base a la categoria-division-subdivision
            df_melted = pd.melt(df, id_vars=['id_categoria', 'id_division', 'id_subdivision'], var_name='fecha', value_name='valor')

            df_melted['fecha'] = pd.to_datetime(df_melted['fecha'], errors='coerce')

            # Eliminamos las dos primeras y tres ultimas filas porque estan vacias
            df_melted = df_melted.iloc[1:-3]

            df_melted[['id_categoria', 'id_division', 'id_subdivision']] = df_melted[['id_categoria', 'id_division', 'id_subdivision']].fillna(0).astype(int)
            df_melted['id_region'] = region
            df_melted = df_melted[['fecha', 'id_region', 'id_categoria', 'id_division', 'id_subdivision', 'valor']]

            # Una vez editado agregamos al df a la lista de dfs editados
            dfs_editados.append(df_melted)
            print(F"DF N{region}")
            print("-------------------------------")
            print(df_melted)
            region += 1


        # Juntamos todos los dfs en uno solo, eliminamos datos incorrectos
        df_juntos = pd.concat(dfs_editados, ignore_index=True)
        print(df_juntos)
        df_juntos['valor'] = df_juntos['valor'].replace('///', None).astype(float)
        
        # Eliminar filas que tienen 0 en los campos 'id_categoria', 'id_division', y 'id_subdivision'
        df_juntos = df_juntos.query('id_categoria != 0 and id_division != 0 and id_subdivision != 0')

        return df_juntos
    
    def df_nacion(self, ruta):
        df_original = pd.read_excel(ruta, sheet_name=2, skiprows=5, nrows=19)
        df_original = df_original.iloc[2:]
        return df_original
    


#Carpeta scrap_IPC
directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')

#Direcciones de los Excels
path_ipc_apertura = os.path.join(ruta_carpeta_files, 'sh_ipc_mes_ano.xls') #Dato por region

df = TransformRegionesValores(None,None,None,None).df_nacion(path_ipc_apertura)

print(df)