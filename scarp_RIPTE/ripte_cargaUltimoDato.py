import time
import mysql.connector
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
import calendar
from datetime import datetime

from informes import InformesRipte

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
        self.conn = mysql.connector.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self.cursor = self.conn.cursor() #--cursor para usar BDD


    #Cargamos los datos
    def loadInDataBase(self):  
        table_name='ripte'
        # Se toma el tiempo de comienzo
        start_time = time.time()
        tolerancia = 1.99         
        
        #Conexion a la BDD
        self.conectar_bdd()
        

        #Carga de pagina
        driver = webdriver.Chrome()
        driver.get('https://www.argentina.gob.ar/trabajo/seguridadsocial/ripte')
       
       #Buscamos la tabla que contiene los datos
        elemento = driver.find_element(By.XPATH, '//*[@id="block-system-main"]/section/article/div/div[9]/div/div/div/div/div[1]/div/h3')
        contenido_texto = elemento.text
        contenido_numerico = contenido_texto.replace('$', '').replace('.','').replace(',', '.')

        try:
            contenido_float = float(contenido_numerico)
            print("Contenido como float:", contenido_float)
        except ValueError:
            print("El contenido no es un número válido.")
            
        # Obtener la última fecha y el último valor de ripte de la base de datos
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT fecha, valor FROM {table_name} ORDER BY fecha DESC LIMIT 1")
        ultima_fecha, ultimo_ripte = cursor.fetchone()

        print("Último ripte en la base de datos:", ultimo_ripte)

        # Convertir la fecha a objeto datetime
        fecha_base = datetime.strptime(str(ultima_fecha), "%Y-%m-%d")

        # Sumar días a la fecha
        nueva_fecha = fecha_base + timedelta(days=calendar.monthrange(fecha_base.year, fecha_base.month)[1])
        print("Nueva fecha:", nueva_fecha.strftime("%Y-%m-%d"))


        if abs(contenido_float - ultimo_ripte) < tolerancia:
            print("El valor de ripte es el mismo, no se agregaron nuevos datos")
        else:
            # Sentencia SQL para insertar los datos en la tabla ripte
            insert_query = f"INSERT INTO {table_name} (fecha, valor) VALUES (%s, %s)"

            # Insertar nuevos datos
            cursor.execute(insert_query, (nueva_fecha, contenido_float))
            self.conn.commit()

            print("Se agregaron nuevos datos")

            InformesRipte(self.host,self.user,self.password,self.database).enviar_mensajes(nueva_fecha, contenido_float, ultimo_ripte)
        cursor.close()
        self.conn.close()