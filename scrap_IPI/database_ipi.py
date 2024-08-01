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


    #Objetivo: obtener tamanos de base de datos, y del df, posteriormente se usa para verificar la carga
    def verificar_carga(self,df):

        #Obtencion del tamano de los datos de la bdd
        table_name= 'ipi'
        select_row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        self.cursor.execute(select_row_count_query)
        tamano_bdd = self.cursor.fetchone()[0]   

        #Obtenemos tamano del DF a cargar
        tamano_df = len(df) 

        return tamano_bdd,tamano_df

    #Objetivo: realizar efectivamente la carga a la BDD
    def cargar_datos(self,df):

        
        #Obtencion de cantidades de datos
        tamano_bdd, tamano_df = self.verificar_carga(df)

        print(tamano_bdd,tamano_df)

        if tamano_df > tamano_bdd:
            
            #Obtenemos solo los datos que cargaremos, que serian los nuevos.
            df_datos_nuevos = df.tail(tamano_df - tamano_bdd)

            #Carga a la BDD
            df_datos_nuevos.to_sql(name="ipi", con=self.engine, if_exists='append', index=False)

            #Cerramos conexiones
            self.conn.commit()
            self.conn.close()
            self.cursor.close()

            print("*****************************************************************")
            print(" *** SE HA PRODUCIDO UNA CARGA DE IPI ***")
            print("*****************************************************************")

            #Retornamos bandera para que se mande el correo
            return True
        else:

            print(" ==== NO EXISTEN DATOS NUEVOS DE IPI ====")

            #Retornamos bandera para que no se envie el correo
            return False


    def main(self,df):
        
        #Conectamos a la bdd
        self.conectar_bdd()

        #Verificamos carga - Obtenemos una bandera para ver si mandamos o no un correo
        bandera = self.cargar_datos(df)

        return bandera

    
