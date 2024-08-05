"""
Archivo destinado a controlar, verificar y ejecutar la carga de datos relacionadas
al IPI NACIONAL.
"""
from pymysql import connect #--> Se usa para hacer consultas a la bdd
from sqlalchemy import create_engine #--> Se usa para cargar la bdd

class Database_ipi:

    #Definicion de atributos
    def __init__(self,host, user, password, database):

        self.host = host
        self.user = user
        self.password = password
        self.database = database

        self.conn = None
        self.cursor = None

        #Se usa para cargar la BDD
        self.engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")


    #Objetivo: Conectar a la bdd con las credenciales proporcionadas
    def conectar_bdd(self):

        self.conn = connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.cursor = self.conn.cursor()


    ## ========================= CARGA DE IPI VARIACIONES ========================= ##

    #Objetivo: obtener tamanos de base de datos, y del df, posteriormente se usa para verificar la carga
    def verificar_carga_variaciones(self,df_variaciones):

        #Obtencion del tamano de los datos de la bdd
        table_name= 'ipi'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        self.cursor.execute(select_row_count_query)
        tamano_bdd = self.cursor.fetchone()[0]   

        #Obtenemos tamano del DF a cargar
        tamano_df = len(df_variaciones) 

        return tamano_bdd,tamano_df

    #Objetivo: realizar efectivamente la carga a la BDD
    def cargar_datos_variaciones(self,df_variaciones):

        
        #Obtencion de cantidades de datos
        tamano_bdd, tamano_df = self.verificar_carga_variaciones(df_variaciones)

        if tamano_df > tamano_bdd:
            
            #Obtenemos solo los datos que cargaremos, que serian los nuevos.
            df_datos_nuevos = df_variaciones.tail(tamano_df - tamano_bdd)

            #Carga a la BDD
            df_datos_nuevos.to_sql(name="ipi", con=self.engine, if_exists='append', index=False)

            #Cerramos conexiones
            self.conn.commit()
            self.conn.close()
            self.cursor.close()

            print("*****************************************************************")
            print(" *** SE HA PRODUCIDO UNA CARGA DE IPI - VARIACIONES INTERANUALES ***")
            print("*****************************************************************")

            #Retornamos bandera para que se mande el correo
            return True
        else:

            print(" ==== NO EXISTEN DATOS NUEVOS DE IPI - VARIACIONES INTERANUALES ====")

            #Retornamos bandera para que no se envie el correo
            return False
        


    ## ========================= CARGA DE IPI VALORES ========================= ##


    #Objetivo: obtener tamanos de base de datos, y del df, posteriormente se usa para verificar la carga
    def verificar_carga_valores(self,df_valores):

        #Obtencion del tamano de los datos de la bdd
        table_name= 'ipi_valores'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        self.cursor.execute(select_row_count_query)
        tamano_bdd = self.cursor.fetchone()[0]   

        #Obtenemos tamano del DF a cargar
        tamano_df = len(df_valores) 

        return tamano_bdd,tamano_df
    
   #Objetivo: realizar efectivamente la carga a la BDD
    def cargar_datos_valores(self,df_valores):

        #Obtencion de cantidades de datos
        tamano_bdd, tamano_df = self.verificar_carga_valores(df_valores)

        print(tamano_bdd,tamano_df)

        #Si hay datos nuevos, CARGAR
        if tamano_df > tamano_bdd:
            
            #Obtenemos solo los datos que cargaremos, que serian los nuevos.
            df_datos_nuevos = df_valores.tail(tamano_df - tamano_bdd)

            #Carga a la BDD
            df_datos_nuevos.to_sql(name="ipi_valores", con=self.engine, if_exists='append', index=False)

            #Cerramos conexiones
            self.conn.commit()
            self.conn.close()
            self.cursor.close()

            print("*****************************************************************")
            print(" *** SE HA PRODUCIDO UNA CARGA DE IPI - VALORES DE SERIE ORIGINAL ***")
            print("*****************************************************************")

            #Retornamos bandera para que se mande el correo
            return True
        
        #Si no hay datos nuevos, NO CARGAR
        else:

            print(" ==== NO EXISTEN DATOS NUEVOS DE IPI - SERIE ORIGINAL ====")

            #Retornamos bandera para que no se envie el correo
            return False



    #Objetivo: funcionar como funcion principal
    def main(self,df_valores,df_variaciones):
        
        #Conectamos a la bdd
        self.conectar_bdd()

        #Verificamos carga - Obtenemos una bandera para ver si mandamos o no un correo || VALORES
        bandera_valores = self.cargar_datos_valores(df_valores)

        #Conectamos a la bdd
        self.conectar_bdd()

        #Verificamos carga - Obtenemos una bandera para ver si mandamos o no un correo || VARIACIONES
        bandera_vars = self.cargar_datos_variaciones(df_variaciones)

        #Resultado final
        bandera = (bandera_valores and bandera_vars)

        return bandera

    
