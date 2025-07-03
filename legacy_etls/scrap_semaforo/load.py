from sqlalchemy import create_engine

class  Database:

    #Inicializacion de variables en la clase
    def __init__(self, host, user, password, database):

        self.host = host
        self.user = user
        self.password = password 
        self.database = database
        self.cursor = None
        self.conn = None


    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE A LAS CONEXIONES ==== #
    # =========================================================================================== #        

    def load_data(self,df_interanual_transformado, df_intermensual_transformado):
                    
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df_interanual_transformado.to_sql(name="semaforo_interanual", con=engine, if_exists='replace', index=False)
        df_intermensual_transformado.to_sql(name="semaforo_intermensual", con=engine, if_exists='replace', index=False)