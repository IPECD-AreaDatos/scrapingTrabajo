from sqlalchemy import create_engine
import mysql
import mysql.connector
import pandas as pd

class uploadDataInDataBase:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def conectar_bdd(self):
        self.conn = mysql.connector.connect(
            host=self.host, user=self.user, password=self.password, database=self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    def load_data(self, df):
        print("\n*****************************************************************************")
        print("***********************Inicio de la seccion Ieric Empresas actividad***********************")
        print("\n*****************************************************************************")
        print(df)

        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="ieric_actividad", con=engine, if_exists='replace', index=False)
        print("Se realizo la carga a la base de datos ieric_actividad en datalake_economico")
        
    def cargaBaseDatos(self, df):
        print("\n*****************************************************************************")
        print("***********************Inicio de la seccion Ieric Trabajo Registrado************************")
        print("\n*****************************************************************************")
        # Suponiendo que 'df' es tu DataFram
        print(df)

        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="ieric_ocupacion", con=engine, if_exists='replace', index=False)

        
        # Confirmar los cambios en la base de datos
        self.conn.commit()
        # Cerrar el cursor y la conexión
        self.cursor.close()
        self.conn.close()
