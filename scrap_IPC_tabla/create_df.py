from sqlalchemy import create_engine
import pandas as pd
import numpy as np


class ExtractDataBDD:

    #Definicion de atributos
    def __init__(self, host, user, password, database):

        self.host = host
        self.user = user
        self.password = password
        self.database = database

        #Engine que conecta a la BDD con SQLAlchemy
        self.engine = None


        #DF total de los IPC's
        self.df = pd.DataFrame(columns=['fecha', 'ipc_general', 'ipc_nea', 'ipc_caba', 'ipc_online', 'ipc_rem'])


    # ===== ZONA DE TOMA DE LOS IPC ===== #

    #Buscamos los valores de IPC ONLINE en la tabla 'ipc_online' del datalake_economico
    def ipc_online(self):
        
        #Cargamos campos 'fecha' y 'ipc_online' que corresponde a las variaciones mensuales
        self.df[['fecha','ipc_online']] = pd.read_sql('SELECT fecha, variacion_mensual FROM ipc_online ORDER BY fecha',con=self.engine)

    #Buscamos los valores de IPC ONLINE en la tabla 'ipc_caba' del datalake_economico
    def ipc_caba(self,fecha_min):

        #Cargamos campo 'ipc_caba' que corresponde a las variaciones mensuales - Usamos la fecha base de IPC ONLINE
        self.df['ipc_caba'] = pd.read_sql(f"SELECT var_mensual_ipc_caba FROM ipc_caba WHERE fecha >= '{fecha_min}'",con=self.engine)


    #Buscamos los valores de IPC REM en la tabla 'ipc_rem_variaciones' del datalake_economico
    def ipc_rem(self,fecha_min):

        # Obtenemos todos los datos de ipc_rem_variaciones
        ipc_rem_variaciones = pd.read_sql(f"SELECT * FROM ipc_rem_variaciones WHERE fecha >= '{fecha_min}'", con=self.engine)
        
        #Aca asignamos valores CON LA MISMA LONGITUD - Osea, faltan datos, que serian los del ultimo mes.
        self.df['ipc_rem'] = ipc_rem_variaciones['var_mensual']

        #Asignamos el ultimo valor de otra forma
        self.df.loc[len(self.df)] = [ipc_rem_variaciones['fecha'].values[-1],None,None,None,None,ipc_rem_variaciones['var_mensual'].values[-1]]

    #SALVADA DE IPC GENERAL Y NEA
    def ipc_salvada(self):

        self.df['ipc_general'] = [19.6,15.0,11.5,9.2,4.3,4.4]
        self.df['ipc_nea'] = [21.7,11.7,8.8,9.1,4.2,4.8]


    
    def extraer_datos(self):

        df = pd.DataFrame( columns= ['fecha', 'ipc_general', 'ipc_nea', 'ipc_caba', 'ipc_online', 'rem'])
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")

        ipc_online_variacion = pd.read_sql_query("SELECT variacion_mensual FROM ipc_online ORDER BY fecha", con=engine) 
        ipc_online_fecha = pd.read_sql_query("SELECT fecha FROM rem_precios_minoristas2", con=engine) 

        df['fecha'] = ipc_online_fecha


        df['ipc_online'] = ipc_online_variacion

        rem_mediana = pd.read_sql_query("SELECT mediana FROM rem_precios_minoristas2", con=engine) 
        df['rem'] = rem_mediana
        df.at[4, 'rem'] = 7.5


        caba = pd.read_sql_query("SELECT var_mensual_ipc_caba FROM ipc_caba order by fecha desc limit 5", con=engine)
        caba = caba.sort_index(ascending=False)
        caba = caba.reset_index(drop=True)

        df['ipc_caba'] = caba
        df.at[5, 'ipc_caba'] = 4.8


        nea = pd.read_sql_query("select vmensual_nea_ipc from dwh_sociodemografico.correo_cbt_cba order by fecha desc limit 5", con=engine)
        nea = nea.sort_index(ascending=False)
        nea = nea.reset_index(drop=True)

        df['ipc_nea'] = nea

        df['ipc_nea'] = pd.to_numeric(df['ipc_nea'], errors='coerce')


        df['ipc_nea'] = df['ipc_nea'] * 100


        
        general = [20.6,13.2,11,8.8,4.2,0]
        
        df['ipc_general']= general
        df.at[5, 'ipc_general'] = np.NaN


        df = df.round({'ipc_general': 1, 'ipc_nea': 1, 'ipc_caba': 1, 'ipc_online': 1, 'rem': 1})

        # Mostrar DataFrame después de redondear
        print("\nDataFrame después de redondear:")

        print(df)
        
        return df
    

    #OBJETIVO: Buscar las variaciones de los distintos IPC - USAMOS COMO FECHA BASE A IPC ONLINE
    def main(self):

        #Conexion a la base de datos
        self.engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")

        #Buscamos datos de IPC ONLINE
        self.ipc_online()

        # -- Fecha base para el resto de columnas, se usara para poner una fecha minima de busqueda en las base de datos
        fecha_min = min(self.df['fecha'])

        #Buscamos datos de IPC CABA
        self.ipc_caba(fecha_min)
        self.ipc_salvada()

        #Buscamos datos de IPC REM
        self.ipc_rem(fecha_min)
        
        print(self.df)


database = 'datalake_economico'
host = '54.94.131.196'
user = 'estadistica'
password = 'Estadistica2024!!'

instancia = ExtractDataBDD(host=host,user=user,password=password,database=database)
instancia.main()
