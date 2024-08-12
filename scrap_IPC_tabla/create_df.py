"""
Este archivo mezcla propiedades de EXTRACT y TRANSFORM.

Extract: Porque accede a la base datos para buscar lo necesario.
Transform: Porque manipula los datos para construir un DF adecuado.
"""

from sqlalchemy import create_engine
import pandas as pd
import sys  

class ExtractDataBDD:

    #Definicion de atributos
    def __init__(self, host, user, password, database):

        self.host = host
        self.user = user
        self.password = password
        self.database = database

        #Engine que conecta a la BDD con SQLAlchemy
        self.engine = None


    # ===== ZONA DE TOMA DE LOS IPC ===== #

    #Buscamos los valores de IPC ONLINE en la tabla 'ipc_online' del datalake_economico
    def ipc_online(self):
        
        #Cargamos campos 'fecha' y 'ipc_online' que corresponde a las variaciones mensuales
        return pd.read_sql('SELECT fecha, variacion_mensual FROM ipc_online ORDER BY fecha',con=self.engine)


    #Buscamos los valores de IPC ONLINE en la tabla 'ipc_caba' del datalake_economico
    def ipc_caba(self,fecha_min):

        #Cargamos campo 'ipc_caba' que corresponde a las variaciones mensuales - Usamos la fecha base de IPC ONLINE
        return pd.read_sql(f"SELECT var_mensual_ipc_caba FROM ipc_caba WHERE fecha >= '{fecha_min}'",con=self.engine)


    #Buscamos los valores de IPC REM en la tabla 'ipc_rem_variaciones' del datalake_economico
    def ipc_rem(self,fecha_min):

        # Obtenemos todos los datos de ipc_rem_variaciones
        return pd.read_sql(f"SELECT var_mensual FROM ipc_rem_variaciones WHERE fecha >= '{fecha_min}'", con=self.engine)
        
    #SALVADA DE IPC GENERAL Y NEA
    def ipc_salvada(self):

        vars_ipc_general =  [20.6,13.2,11.0,8.8,4.2,4.6]
        vars_ipc_nea = [19.5,10.9,10.3,6.3,3.7,4.4]

        return vars_ipc_general, vars_ipc_nea

    
    """
    Objetivo: construir el DF a cargar. Estrategia: Buscar el DF mas largo, y en base a su tamano
    generar un df de esa cantidad de filas. Con eso vamos insertando cada cosa en su lugar.
    
    """
    def construir_df(self,df_online,df_caba,df_rem,df_ipc_regional,fecha_min):

        #Obtenemos la cantidad mas grande de los largos de los dataframes
        lista_cantidades = [len(df_online),len(df_caba),len(df_rem),len(df_ipc_regional)] #--> Lista que representa las cantidad de datos de cada DF
        data_max = max(lista_cantidades)

        #DF total de los IPC's - Tomamos el largo antes indicado
        df = pd.DataFrame(columns=['fecha', 'ipc_nacion', 'ipc_nea', 'ipc_caba', 'ipc_online', 'ipc_rem'],index=range(data_max))

        #Asignamos valores de df_online
        df.loc[:len(df_online)-1,'ipc_online'] = df_online['variacion_mensual'].values

        #Asignacion de valores de df_caba
        df.loc[:len(df_caba)-1,'ipc_caba'] = df_caba['var_mensual_ipc_caba'].values

        #Asignacion de IPC general y NEA
        df.loc[:len(df_ipc_regional)-1,['ipc_nacion','ipc_nea']] = df_ipc_regional[['ipc_nacion','ipc_nea']].values

        #Asignacion de IPC del REM
        df.loc[:len(df_rem)-1,'ipc_rem'] = df_rem['var_mensual'].values


        #Asignacion de las fechas, usando IPC ONLINE COMO BASE
        fechas = pd.date_range(start=fecha_min, periods=len(df), freq='MS')# Crear un rango de fechas mensual a partir de fecha_min
        df['fecha'] = fechas

        return df
    


    #Objetivo: calcular los acumulados para cada mes
    def calcular_acumulados(self,df):


        #Nacion
        df['ipc_nacion_acum'] = ((((df['ipc_nacion']/100) + 1).cumprod()) - 1) * 100

        #NEA 
        df['ipc_nea_acum'] = ((((df['ipc_nea']/100) + 1).cumprod()) - 1) * 100

        #CABA
        df['ipc_caba_acum'] = ((((df['ipc_caba']/100) + 1).cumprod()) - 1) * 100

        #ONLINE
        df['ipc_online_acum'] = ((((df['ipc_online']/100) + 1).cumprod()) - 1) * 100

        #REM
        df['ipc_rem_acum'] = ((((df['ipc_rem']/100) + 1).cumprod()) - 1) * 100
       
        


    #OBJETIVO: Buscar las variaciones de los distintos IPC - USAMOS COMO FECHA BASE A IPC ONLINE
    def main(self):

        #Conexion a la base de datos
        self.engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")

        #1) Buscamos datos de IPC ONLINE, y buscamos la fecha base
        df_online = self.ipc_online()

        # -- Fecha base para el resto de columnas, se usara para poner una fecha minima de busqueda en las base de datos.
        fecha_min = min(df_online['fecha'])
        

        #2) Buscamos el resto de los datos

        #Datos de CABA
        df_caba = self.ipc_caba(fecha_min)

        #Datos del REM
        df_rem = self.ipc_rem(fecha_min)
        
        #Datos del IPC - Actualmente falta los datos del IPC reales de las variaciones, por eso esto hay que corregir. Fecha = 30/7
        df_ipc_regional = pd.DataFrame()
        ipc_nacion,ipc_nea =  self.ipc_salvada()

        df_ipc_regional['ipc_nacion'] = ipc_nacion
        df_ipc_regional['ipc_nea'] = ipc_nea

        #3) Construccion final del dataframe
        df = self.construir_df(df_online,df_caba,df_rem,df_ipc_regional,fecha_min)


        #4)Calculo de acumulados
        self.calcular_acumulados(df)

        print(df)
        return df
    