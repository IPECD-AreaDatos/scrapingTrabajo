from sqlalchemy import create_engine
from pymysql import connect
class Database:

    #Inicializacion de variables en la clase
    def __init__(self, host, user, password, database):

        self.host = host
        self.user = user
        self.password = password 
        self.database = database
        self.cursor = None
        self.conn = None


    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE A LOS SETTERs ==== #
    # =========================================================================================== #  

    #Objetivo: cambiar el nombre de la base de datos para reconectarnos a otra.
    def set_database(self,new_name):

        self.database = new_name


    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE A LAS CONEXIONES ==== #
    # =========================================================================================== #        

    #Objetivo: conectar a la base de datos
    def connect_db(self):

            self.conn = connect(
                host=self.host, user=self.user, password=self.password, database=self.database
            )
            self.cursor = self.conn.cursor()


    #Objetivo: guardar los ultimos cambios hechos y cerrar las conexiones
    def close_connections(self):
        self.conn.commit()
        self.conn.close()
        self.cursor.close()


    #Objetivo: obtener los datos del dataframe, y de la tabla almacenada en la bdd
    def check_lens(self,df):
        
        # Verificar cuantas filas tiene la tabla de mysql ejecutando la consulta
        select_query = "SELECT COUNT(*) FROM indicadores_salarios"
        self.cursor.execute(select_query)

        #Tamaño de la tabla de la BDD
        len_bdd = self.cursor.fetchone()[0]

        #Tamaño del dataframe
        len_df = len(df)

        return len_bdd,len_df
    

    def load_data(self,df):
    
        self.connect_db()
        len_bdd,len_df = self.check_lens(df)

        if len_df > len_bdd:

            #Obtenemos la diferencia de filas
            df = df.tail(len_df - len_bdd)
            
            #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
            df.to_sql(name="indicadores_salarios", con=engine, if_exists='append', index=False)
            print("carga realizada")
            #Guardamos cambios 
            self.conn.commit()
            self.close_connections()

            #Se retorna true para enviar el correo
            return True
        else:
            print("\n - No existen datos nuevos de INDICE DE SALARIOS para cargar \n")
            return False
