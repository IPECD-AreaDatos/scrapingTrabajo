import mysql.connector
from sqlalchemy import create_engine
from datos_deflactados import Deflactador
import os

class conexionBaseDatos:

    def __init__(self, host, user, password, database):
        
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    #Conexion a la base de datos
    def conectar_bdd(self,host,user,password,database):

        self.conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )

        self.cursor = self.conn.cursor()


    def cargar_datos(self,df):
        
        #Conectamos a la base de datos
        self.conectar_bdd(self.host,self.user,self.password,self.database)


        #Definimos querys que vamos a utilizar
        nombre_tabla = 'supermercado_encuesta'
        delete_query ="TRUNCATE `datalake_economico`.`supermercado_encuesta`"
        query_cantidad_datos = f'SELECT COUNT(*) FROM {nombre_tabla}'

        print(self.validacion_de_carga(df,query_cantidad_datos)) 
        #Si las cantidad de filas del DF descargado, es mayor que el ya almacenado --> Realizar carga
        if (self.validacion_de_carga(df,query_cantidad_datos)):

            #Eliminamos tabla - son datos estimativos
            self.cursor.execute(delete_query)

            #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
            df.to_sql(name="supermercado_encuesta", con=engine, if_exists='append', index=False)

            #Deflactacion de datos
            Deflactador(self.host,self.user,self.password,self.database).main()

            print("""

            ================================================================================
             *** SE HA PRODUCIDO UNA ACTUALIZACION EN LAS ENCUESTAS DE SUPERMERCADOS ***
            ================================================================================

            """)

        else:
            print(f"NO HAY CAMBIOS EN LOS DATOS DE {nombre_tabla}")
                

    
    #Realizamos una validacion de carga comparando el tamaño del dataframe con la cantidad de datos almacenados
    def validacion_de_carga(self,df,query_cantidad_datos):

        #Obtencion de tamaño del DF sacado del EXCEL
        cant_filas_df = len(df)

        #Obtencion de cantidad de filas de 
        self.cursor.execute(query_cantidad_datos)
        row_count_before = self.cursor.fetchone()[0]

        print("Cantidad de filas del Df:",cant_filas_df)
        print("cantidad de filas de la bdd:",row_count_before)

        return (cant_filas_df > row_count_before)


   