import mysql
import mysql.connector
import datetime
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd
from datetime import datetime
import calendar
import os
import xlrd
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
        # Suponiendo que 'df' es tu DataFram
        #print(df)

        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="censo_ipecd", con=engine, if_exists='replace', index=False)

        # Confirmar los cambios en la base de datos
        self.conn.commit()
        # Cerrar el cursor y la conexión
        self.cursor.close()
        self.conn.close()

