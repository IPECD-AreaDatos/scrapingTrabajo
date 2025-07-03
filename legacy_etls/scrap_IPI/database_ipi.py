"""
Archivo destinado a controlar, verificar y ejecutar la carga de datos relacionadas
al IPI NACIONAL.
"""
from pymysql import connect #--> Se usa para hacer consultas a la bdd
from sqlalchemy import create_engine #--> Se usa para cargar la bdd
import pandas as pd

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



    ## ========================= CARGA DE IPI EN SUS DINTINTAS TABLAS ========================= ##


    #Objetivo: obtener tamanos de base de datos, y del df, posteriormente se usa para verificar la carga
    def verificar_carga(self,df,name_table):

        #Obtencion del tamano de los datos de la bdd
        select_row_count_query = f"SELECT COUNT(*) FROM {name_table}"
        self.cursor.execute(select_row_count_query)
        tamano_bdd = self.cursor.fetchone()[0]   

        #Obtenemos tamano del DF a cargar
        tamano_df = len(df) 

        return tamano_bdd,tamano_df
    
   #Objetivo: realizar efectivamente la carga a la BDD
    def cargar_datos(self,df,name_table):

        #Obtencion de cantidades de datos
        tamano_bdd, tamano_df = self.verificar_carga(df,name_table)

        print(tamano_bdd,tamano_df)

        #Si hay datos nuevos, CARGAR
        if tamano_df > tamano_bdd:
            
            #Obtenemos solo los datos que cargaremos, que serian los nuevos.
            df_datos_nuevos = df.tail(tamano_df - tamano_bdd)

            #Carga a la BDD
            df_datos_nuevos.to_sql(name=f"{name_table}", con=self.engine, if_exists='append', index=False)

            print("*****************************************************************")
            print(f" *** SE HA PRODUCIDO UNA CARGA DE IPI - TABLA {name_table} ***")
            print("*****************************************************************")

            #Retornamos bandera para que se mande el correo
            return True
        
        #Si no hay datos nuevos, NO CARGAR
        else:

            print(f" ==== NO EXISTEN DATOS NUEVOS DE IPI - {name_table} ====")

            #Retornamos bandera para que no se envie el correo
            return False



    #Objetivo: funcionar como funcion principal
    def main(self,df_valores,df_variaciones,df_var_inter_acum):
        
        #Conectamos a la bdd
        self.conectar_bdd()


        #Verificamos carga - Obtenemos una bandera para ver si mandamos o no un correo || VALORES
        bandera_valores = self.cargar_datos(df_valores,'ipi_valores')
        bandera_variaciones = self.cargar_datos(df_variaciones,'ipi_variacion_interanual')
        bandera_var_inter_acum = self.cargar_datos(df_var_inter_acum,'ipi_var_interacum')

        self.conn.commit()
        self.conn.close()
        self.cursor.close()


        #Resultado final
        bandera = (bandera_valores and bandera_variaciones and bandera_var_inter_acum)

        return bandera

    
