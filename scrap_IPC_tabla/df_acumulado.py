import mysql
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import numpy as np


class Create_Df_Acum:

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
    
    def armarAcum(self):
        resultados_acumulados_df = pd.DataFrame( columns= ['fecha', 'ipc_general', 'ipc_nea', 'ipc_caba', 'ipc_online', 'rem'])
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")

        tabla_ipc = pd.read_sql_query("SELECT * FROM tabla_ipc", con=engine) 

        print(tabla_ipc)

        resultados_acumulados_df.loc[0, 'fecha'] = '2024'
        
        # Calcular los valores acumulados para cada columna y asignarlos correctamente
        for columna in tabla_ipc.columns:
            if columna != 'fecha':  # Excluir la columna de fecha
                acumulado = 1.0  # Inicializar acumulado en 1 para el producto
                for valor in tabla_ipc[columna].dropna():
                    acumulado *= (valor / 100) + 1  # Multiplicar por cada valor ajustado
                acumulado = (acumulado - 1) * 100  # Calcular acumulado final
                print("acum:", acumulado)

                resultados_acumulados_df.loc[0, columna] = acumulado

        # Mostrar los resultados acumulados
        print("Resultados acumulados:")
        resultados_acumulados_df = resultados_acumulados_df.round({'ipc_general': 1, 'ipc_nea': 1, 'ipc_caba': 1, 'ipc_online': 1, 'rem': 1})

        print(resultados_acumulados_df)
        return resultados_acumulados_df