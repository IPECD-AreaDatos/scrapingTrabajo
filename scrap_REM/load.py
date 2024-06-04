import mysql
import mysql.connector
from sqlalchemy import create_engine
import pandas as pd


class conexcionBaseDatos:

    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
    
    def conectar_bdd(self):
        self.conn = mysql.connector.connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    def cargaBaseDatos(self, df):
        print("\n*****************************************************************************")
        print("***********************Inicio de REM precios minoristas************************")
        print("\n*****************************************************************************")
        # Suponiendo que 'df' es tu DataFram
        print(df)
        query_delete = 'TRUNCATE rem_precios_minoristas'
        self.cursor.execute(query_delete)
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
       
    
        df.to_sql(name="rem_precios_minoristas", con=engine, if_exists='replace', index=False)

        
    def cargaBaseDatos2(self, df):
        print("\n*****************************************************************************")
        print("*************************Inicio de REM cambio nominal**************************")
        print("\n*****************************************************************************")
        # Suponiendo que 'df' es tu DataFram
        print(df)
        query_delete = 'TRUNCATE rem_cambio_nominal'
        self.cursor.execute(query_delete)
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
       
    
        df.to_sql(name="rem_cambio_nominal", con=engine, if_exists='replace', index=False)

        
        # Confirmar los cambios en la base de datos
        self.conn.commit()
        # Cerrar el cursor y la conexión
        self.cursor.close()
        self.conn.close()
        
    
        
