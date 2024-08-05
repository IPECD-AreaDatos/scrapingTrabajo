import mysql
import mysql.connector
from pymysql import connect
from sqlalchemy import create_engine
import datetime
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd
from armadoXLSDataNacion import LoadXLSDataNacion
from datetime import datetime
import calendar
import os
import xlrd

class Correo: 
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
    
    def conectar_bdd(self):
        self.conn = connect(
            host = self.host, user = self.user, password = self.password, database = self.database
        )
        self.cursor = self.conn.cursor()
        return self