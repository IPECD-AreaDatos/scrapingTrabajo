import numpy as np
import pandas as pd
from datetime import datetime
import mysql
import mysql.connector
from dateutil.relativedelta import relativedelta
from informes import InformesEmae
from sqlalchemy import create_engine, text
from dateutil.relativedelta import relativedelta

class cargaIndice:

    def __init__(self):

        self.conn = None
        self.cursor = None 

    def conecta_bdd(self,host, user, password, database):

        # Conexión a la base de datos MySQL
        self.conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        self.cursor = self.conn.cursor()

    def loadXLSIndiceEMAE(self, file_path, lista_fechas, lista_SectorProductivo, lista_valores, host, user, password, database):
        # Leer el archivo Excel en un DataFrame de pandas
        df = pd.read_excel(file_path, sheet_name=0, skiprows=2)
        df = df.replace({pd.NA: None})  # Reemplazar los valores NaN por None
        df = df.drop(df.index[-1])  # Eliminar la última fila que contiene "Fuente: INDEC"
        df = df.drop(df.index[-1])  # Eliminar otra fila no deseada

        # Obtener las columnas desde C hasta R
        columnas_valores = df.columns[2:18]
        lista_columnas = list(columnas_valores)

        # Inicializar listas para datos
        lista_fechas = []
        lista_valores = []
        lista_SectorProductivo = []

        # Calcular las fechas
        fecha_inicio = datetime(2004, 1, 1)
        num_meses = len(df) - 2  # Restar 2 para compensar las filas de encabezados
        lista_fechas_base = [fecha_inicio + relativedelta(months=i) for i in range(num_meses)]

        # Recopilar datos
        for index, row in df.iterrows():
            if index >= 2:  # Fila 3 en adelante
                fecha = lista_fechas_base[index - 2]  # Restar 2 para compensar el índice
                for columna in columnas_valores:
                    valor = df.at[index, columna]
                    indice_sector_productivo = lista_columnas.index(columna) + 1

                    lista_valores.append(valor)
                    lista_SectorProductivo.append(indice_sector_productivo)
                    lista_fechas.append(fecha)  # Añadir la fecha correspondiente

        # Crear DataFrame para revisión
        datos = {
            'fecha': lista_fechas,  
            'sector_productivo': lista_SectorProductivo,
            'valor': lista_valores
        }
        df_final = pd.DataFrame(datos)

        # Conectar a la base de datos
        url_conexion = f'mysql+pymysql://{user}:{password}@{host}/{database}'
        engine = create_engine(url_conexion)
        with engine.connect() as connection:
            # Verificar cantidad de filas anteriores
            select_row_count_query = text("SELECT COUNT(*) FROM emae")
            row_count_before = connection.execute(select_row_count_query).scalar()

            if len(df_final) > row_count_before:
                # Truncar la tabla y cargar datos
                truncate_query = text("TRUNCATE TABLE emae")
                connection.execute(truncate_query)
                df_final.to_sql('emae', con=engine, index=False, if_exists='append')
                print("Se agregaron nuevos datos")
            else:
                print("Se realizó una verificación de la base de datos")
            
        connection.close()  # Cerrar conexión a la base de datos
            
    def loadXLSVariacionEMAE(self, file_path_variacion, lista_fechas, host, user, password, database):
        # Leer el archivo Excel en un DataFrame de pandas
        df = pd.read_excel(file_path_variacion, sheet_name=0, skiprows=2, usecols="D,F")  # Leer el archivo XLSX y crear el DataFrame
        df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None
        # Eliminar la última fila que contiene "Fuente: INDEC"
        df = df.drop(df.index[-1])
        df = df.drop(df.index[-1])
        # Reemplazar los valores None por 0
        df = df.iloc[1:]
        df = df.fillna(0)

        df = df.rename(columns={
        "Var % respecto a igual período del año anterior": "variacion_interanual",
        "Var % respecto al mes anterior": "variacion_mensual"
        })

        # Agregar una columna con fechas desde enero de 2004
        fecha_inicio = datetime(2004, 1, 1)
        num_filas = len(df)
        lista_fechas = [fecha_inicio + relativedelta(months=i) for i in range(num_filas)]

        # Asegurarse de que la longitud de lista_fechas coincida con la cantidad de filas del DataFrame
        if len(lista_fechas) == len(df):
            df.insert(0, "fecha", lista_fechas)
            
            # Crear una conexión a la base de datos utilizando SQLAlchemy
            url_conexion = f'mysql+pymysql://{user}:{password}@{host}/{database}'
            engine = create_engine(url_conexion)

            # Verificar la cantidad de filas antes de la inserción
            with engine.connect() as connection:
                select_row_count_query = text("SELECT COUNT(*) FROM emae_variaciones")
                row_count_before = connection.execute(select_row_count_query).scalar()
                if len(df) > row_count_before:
                    # Truncar la tabla antes de la inserción
                    truncate_query = text("TRUNCATE TABLE emae_variaciones")
                    connection.execute(truncate_query)
                    # Cargar el DataFrame en la tabla MySQL
                    df.to_sql('emae_variaciones', con=engine, index=False, if_exists='append')
                    print("Se agregaron nuevos datos")
                    InformesEmae(host, user, password, database).enviar_mensajes()
                else:
                    print("Se realizó una verificación de la base de datos")
        else:
            print("Las longitudes no coinciden.")
        

        

