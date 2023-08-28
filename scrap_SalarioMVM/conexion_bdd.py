import mysql
import mysql.connector
import datetime
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd


class conexionBaseDatos:

    def __init__(self,host, user, password, database):

        self.host = host
        self.user = user
        self.password = password
        self.database = database
        

    def conectar_bdd(self):
            
        try:

            conn = mysql.connector.connect(
                host=self.host, user= self.user, password=self.password, database=self.database
            )
            cursor = conn.cursor() #--cursor para usar BDD
        

        except Exception as e:

            print("ERROR:",e)