import time
import pymysql
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
import calendar
from datetime import datetime

from sendMail import InformeRipte

class ripte_cargaUltimoDato:

    #Inicializacion de variables
    def __init__(self, host, user, password, database):
        self.driver = None
        self.conn = None
        self.host = host
        self.user = user
        self.password = password
        self.database = database


    #Establecemos la conexion a la BDD
    def conectar_bdd(self):
        self.conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self.cursor = self.conn.cursor() #--cursor para usar BDD


    #Cargamos los datos
    def loadInDataBaseDatalakeEconomico(self, valor_ripte):  
        table_name='ripte'
        # Se toma el tiempo de comienzo
        tolerancia = 100         
        
        #Conexion a la BDD
        self.conectar_bdd()
            
        # Obtener la última fecha y el último valor de ripte de la base de datos
        cursor = self.conn.cursor()

        cursor.execute(f"SELECT fecha, valor FROM {table_name} ORDER BY fecha DESC LIMIT 1")
        ultima_fecha, ultimo_ripte = cursor.fetchone()

        print("Último ripte en la base de datos:", ultimo_ripte)

        # Convertir la fecha a objeto datetime
        fecha_base = datetime.strptime(str(ultima_fecha), "%Y-%m-%d")

        valor_ripte= float(valor_ripte)

        if abs(valor_ripte - ultimo_ripte) < tolerancia:
            print("El valor de ripte es el mismo, no se agregaron nuevos datos")
        else:
            # Sentencia SQL para insertar los datos en la tabla ripte
            insert_query = f"INSERT INTO {table_name} (fecha, valor) VALUES (%s, %s)"
            # Sumar días a la fecha
            nueva_fecha = fecha_base + timedelta(days=calendar.monthrange(fecha_base.year, fecha_base.month)[1])
            # Insertar nuevos datos
            cursor.execute(insert_query, (nueva_fecha, valor_ripte))
            self.conn.commit()

            print("Se agregaron nuevos datos")
            print(f"nueva fecha {nueva_fecha}")

            InformeRipte(self.host,self.user,self.password,self.database).enviar_mensajes(nueva_fecha, valor_ripte, ultimo_ripte)
        cursor.close()
        self.conn.close()