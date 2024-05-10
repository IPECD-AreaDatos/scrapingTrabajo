import mysql
import mysql.connector
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
        self.conn = mysql.connector.connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    def cargaBaseDatos(self, df):
        print("\n*****************************************************************************")
        print("***********************Inicio de la seccion venta Combustible************************")
        print("\n*****************************************************************************")
        # Suponiendo que 'df' es tu DataFram
        print(df)

        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
       
        # Divide el DataFrame en partes de un millón de filas cada una
        division = 1000000
        df_fraccionado = [df[i:i+division] for i in range(0, len(df), division)]

        # Sube cada parte del DataFrame a la base de datos
        for i, df_fraccionado in enumerate(df_fraccionado):
            df_fraccionado.to_sql(name="combustible", con=engine, if_exists='append', index=False)
            print(f"Parte {i+1} subida a la base de datos.")
        
