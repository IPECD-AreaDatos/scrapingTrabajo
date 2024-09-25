import mysql.connector
import pandas as pd
from sqlalchemy import create_engine

class loadDataBase:

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
    
    def carga_bdd(self, df):
        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="censo_ipecd_municipios", con=engine, if_exists='append', index=False)

        print("Datos cargados!")

        # Confirmar los cambios en la base de datos
        self.conn.commit()
        # Cerrar el cursor y la conexión
        self.cursor.close()
        self.conn.close()
