from pymysql import connect
from sqlalchemy import create_engine

class conexcionBaseDatos:

    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def conectar_bdd(self):
        self.conn = connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    #Objetivo: obtener tamanos de base de datos, y del df, posteriormente se usa para verificar la carga
    def verificar_carga(self, df):

        #Obtencion del tamano de los datos de la bdd
        select_row_count_query = f"SELECT COUNT(*) FROM OEDE_valores"
        self.cursor.execute(select_row_count_query)
        tamano_bdd = self.cursor.fetchone()[0]   

        #Obtenemos tamano del DF a cargar
        tamano_df = len(df) 

        return tamano_bdd,tamano_df
    
    #Objetivo: realizar efectivamente la carga a la BDD
    def cargaBaseDatos(self, df):

        print("\n*****************************************************************************")
        print(f"*******************Inicio de la seccion oede valores *************************")
        print("\n*****************************************************************************")

        #Obtencion de cantidades de datos
        tamano_bdd, tamano_df = self.verificar_carga(df)
        
        print(f" - Cantidad de datos en la base de datos: {tamano_bdd}")
        print(f" - Cantidad de datos extraidos: {tamano_df}")
    
        #Si existen mas datos extraidos, se produce la carga
        if tamano_df > tamano_bdd:
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
            df_tail = df.tail(tamano_df - tamano_bdd)
            df_tail.to_sql(name='OEDE_valores', con=engine, if_exists='append', index=False)

            print("*************")
            print(f" == ACTUALIZACION == Nuevos datos en la base de oede valores")
            print("*************")
        else:
            print("*********")
            print(f"No existen datos nuevos de OEDE")
            print("*********")
    
    def main(self, df):

        #Nos conectamos a la BDD
        self.conectar_bdd()
        self.cargaBaseDatos(df)

        #Cerramos conexiones
        self.conn.commit()
        self.conn.close()
        self.cursor.close()