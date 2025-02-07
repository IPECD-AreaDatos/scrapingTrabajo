from pymysql import connect
from sqlalchemy import create_engine

class ConexionBase:
    
    # definimos variables iniciales para la conexion
    def __init__(self, host, user, password, database):

        self.host = host
        self.user = user
        self.password = password
        self.database = database

    # realizamos la conexion a la base de datos
    def conectar_bdd(self):

        self.conn = connect(
            host=self.host, user=self.user, password=self.password, database=self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    # realizamos la carga a la base de datos
    def carga_bdd(self,df):

        print("\n*****************************************************************************")
        print(f"****************inicio de la carga cobertura y financiacion*******************")
        print("\n*****************************************************************************")

        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name='cobertura_financiacion', con=engine, if_exists='replace', index=False)
    
        print(f" == ACTUALIZACION == Nuevos datos en la base de cobertura y financiacion")

    def main(self, df):

        self.conectar_bdd()
        self.carga_bdd(df)

        # cerramos conexiones
        self.conn.commit()
        self.conn.close()
        self.cursor.close()
