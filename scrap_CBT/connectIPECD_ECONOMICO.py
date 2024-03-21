import pandas as pd
from sqlalchemy import create_engine
import pymysql


class connection_db_ipecd_economico:

    def __init__(self,mysql_host,mysql_user, mysql_password ,database):

        self.mysql_host = mysql_host
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.database = database
        self.tunel = None
        self.conn = None
        self.cursor = None

    # =========================================================================================== #
                    # ==== SECCION CORRESPONDIENTE A SETTERS Y GETTERS ==== #
    # =========================================================================================== #
            
    #Objetivo: cambiar el nombre de la base de datos para reconectarnos a otra.
    def set_database(self,new_name):

        self.database = new_name


    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE A LAS CONEXIONES ==== #
    # =========================================================================================== #
        

    #Conexion a la BDD
    def connect_db(self):

        self.conn = pymysql.connect(
            host = self.mysql_host,
            user = self.mysql_user,
            password = self.mysql_password,
            database = self.database
        )

        self.cursor = self.conn.cursor()
            
    def close_conections(self):

        # Confirmar los cambios en la base de datos y cerramos conexiones
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
        
    def load_datalake(self,df):
        
        #Obtenemos los tamanios
        tamanio_df,tamanio_bdd = self.determinar_tamanios(df)

        if tamanio_df > tamanio_bdd: #Si el DF es mayor que lo almacenado, cargar los datos nuevos
            
            #Es necesario el borrado ya que posteriormente las estimaciones tendremos que recalcularlas
            delete_query_datalake = 'TRUNCATE canasta_basica'
            self.cursor.execute(delete_query_datalake)


            self.cargar_tabla_datalake(df) 
            print("==== SE CARGARON DATOS NUEVOS CORRESPONDIENTES A CBT Y CBA DEL DATALAKE ====")
            
        else: #Si no hay datos nuevos AVISAR
            
            print("==== NO HAY DATOS NUEVOS CORRESPONDIENTES A CBT Y CBA DEL DATALAKE ====")   

    #Objetivo: Obtener tamaños de los datos para realizar verificaciones de varga
    def determinar_tamanios(self,df):

        #Obtenemos la cantidad de datos almacenados
        query_consulta = "SELECT COUNT(*) FROM canasta_basica"
        self.cursor.execute(query_consulta) #Ejecutamos la consulta
        tamanio_bdd = self.cursor.fetchone()[0] #Obtenemos el tamaño de la bdd
        
        #Obtenemos la cantidad de datos del dataframe construido
        tamanio_df = len(df)
        
        print(tamanio_bdd,tamanio_df)

        return tamanio_df,tamanio_bdd

    #Objetivo: almacenar en la tabla cbt_cba con los datos nuevos
    def cargar_tabla_datalake(self,df_cargar):

        query_insertar_datos = "INSERT INTO canasta_basica VALUES (%s, %s, %s, %s, %s,%s, %s)"

        for index,row in df_cargar.iterrows():
            
            #Obtenemos los valores de cada fila del DF
            values = (row['Fecha'],row['CBA_Adulto'],row['CBT_Adulto'],row['CBA_Hogar'],row['CBT_Hogar'],row['cba_nea'],row['cbt_nea'])


            # Convertir valores NaN a None --> Lo hacemos porque los valores 'nan' no son reconocidos por MYSQL
            values = [None if pd.isna(v) else v for v in values]

            #Realizamos carga
            self.cursor.execute(query_insertar_datos,values)

        #Cerramos conexiones
        self.close_conections()
"""
host = '172.17.22.23'
user = 'team-datos'
password = 'HCj_BmbCtTuCv5}'
database = 'ipecd_economico'

instancia = connection_db(host,user,password,database)
instancia.conectar_bdd()
instancia.persona_individual_familia()
"""