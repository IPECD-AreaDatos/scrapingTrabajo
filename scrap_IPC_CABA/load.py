import pymysql
from sqlalchemy import create_engine

class Load:
        
    def __init__(self,host,user,password,database):

        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
        
    #Conexion a la BDD
    def connect_db(self):

            self.conn = pymysql.connect(
                host = self.host,
                user = self.user,
                password = self.password,
                database = self.database
            )

            self.cursor = self.conn.cursor()

    def close_conections(self):

        # Confirmar los cambios en la base de datos y cerramos conexiones
        self.conn.commit()
        self.cursor.close()
        self.conn.close()



    def load_datalake(self,df):

        #Nos conectamos a la bdd 
        self.connect_db()

        #Creamos el delete para ir recargando los datos
        query_delete = 'TRUNCATE ipc_caba'
        self.cursor.execute(query_delete)

        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="ipc_caba", con=engine, if_exists='append', index=False)


