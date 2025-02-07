import pymysql
import pandas as pd
from sqlalchemy import create_engine

class loadDataBaseNuestra:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def conectar_bdd(self):
        self.conn = pymysql.connect(
            host=self.host, user=self.user, password=self.password, database=self.database
        )
        self.cursor = self.conn.cursor()
        return self

    def get_count_from_db(self):
        query = "SELECT COUNT(*) FROM reconocimientos_medicos"
        self.cursor.execute(query)
        count = self.cursor.fetchone()[0]
        return count

    def loadDataBaseNuestra(self, df):
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name='reconocimientos_medicos', con=engine, if_exists='replace', index=False)

    def cerrar_conexion(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()