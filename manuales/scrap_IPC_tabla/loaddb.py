from pymysql import connect
from sqlalchemy import create_engine


class Load:

    #Inicializacion de atributos
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

        #Var relacionadas a las conexiones de la bdd
        self.conn = None
        self.cursor = None
    

    #Objetivo: establecer la conexion a la bdd
    def conectar_bdd(self):

        self.conn = connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()
        return self
    

    #Objetivo: efectuar carga sobre el dwh_economico
    def load_bdd(self, df):
        
        query_delete = 'TRUNCATE tabla_ipc_acumulados'
        self.cursor.execute(query_delete)

        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/dwh_economico")

        df.to_sql(name="tabla_ipc_acumulados", con=engine, if_exists='append', index=False)


        print("\n*****************************************************************************")
        print("***********************CARGA de de Tabla IPC acumulado y variaciones mensuales ************************")
        print("\n*****************************************************************************")



    def main(self,df):
        
        self.conectar_bdd()
        self.load_bdd(df)
    

    # ==== 