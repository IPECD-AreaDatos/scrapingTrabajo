import mysql
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import numpy as np


class Create_Df:

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
    
    def extraer_datos(self):
        df = pd.DataFrame( columns= ['fecha', 'ipc_general', 'ipc_nea', 'ipc_caba', 'ipc_online', 'rem'])
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")

        ipc_online_variacion = pd.read_sql_query("SELECT variacion_mensual FROM ipc_online", con=engine) 
        ipc_online_fecha = pd.read_sql_query("SELECT fecha FROM rem_precios_minoristas2", con=engine) 

        df['fecha'] = ipc_online_fecha

        print(df)
        df['ipc_online'] = ipc_online_variacion

        rem_mediana = pd.read_sql_query("SELECT mediana FROM rem_precios_minoristas2", con=engine) 
        df['rem'] = rem_mediana
        print(df)

        caba = pd.read_sql_query("SELECT var_mensual_ipc_caba FROM ipc_caba order by fecha desc limit 5", con=engine)
        caba = caba.sort_index(ascending=False)
        caba = caba.reset_index(drop=True)

        df['ipc_caba'] = caba
        print(df)

        nea = pd.read_sql_query("select vmensual_nea_ipc from dwh_sociodemografico.correo_cbt_cba order by fecha desc limit 4", con=engine)
        nea = nea.sort_index(ascending=False)
        nea = nea.reset_index(drop=True)

        df['ipc_nea'] = nea

        df.at[4, 'ipc_nea'] = 0.037

        df['ipc_nea'] = df['ipc_nea']* 100
        

        
        general = [20.5,13.2,11,8.7,4.3,0]
        
        df['ipc_general']= general
        df.at[5, 'ipc_general'] = np.NaN

        print(df)
        return df
