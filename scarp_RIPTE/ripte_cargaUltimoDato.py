import time
import mysql.connector
from selenium import webdriver
from selenium.webdriver.common.by import By
from datetime import datetime, timedelta
import calendar
from email.message import EmailMessage
import ssl
import smtplib
import os


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
        conn = mysql.connector.connect(host=self.host, user=self.user, password=self.password, database=self.database)

    #Cargamos los datos
    def loadInDataBase(self):  

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
        cursor.execute("SELECT Fecha, ripte FROM ripte ORDER BY Fecha DESC LIMIT 1")
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
            insert_query = "INSERT INTO ripte (Fecha, ripte) VALUES (%s, %s)"

            # Insertar nuevos datos
            cursor.execute(insert_query, (nueva_fecha, contenido_float))
            conn.commit()

            print("Se agregaron nuevos datos")
            enviar_correo(nueva_fecha, contenido_float, ultimo_ripte)  # Pasar los valores a la función

        cursor.close()
        conn.close()
            
            
            
def enviar_correo(nueva_fecha, nuevo_valor, valor_anterior):  # Recibir los valores como argumentos
    email_emisor = 'departamientoactualizaciondato@gmail.com'
    email_contraseña = 'cmxddbshnjqfehka'
    email_receptores = ['gastongrillo2001@gmail.com', 'matizalazar2001@gmail.com']
    asunto = 'Modificación en la base de datos'
    mensaje = f'''\
        Se ha producido una modificación en la base de datos de RIPTE.
        Nueva fecha: {nueva_fecha}
        Nuevo valor: ${nuevo_valor}
        Valor anterior: ${valor_anterior}
        '''
    
    em = EmailMessage()
    em['From'] = email_emisor
    em['To'] = ", ".join(email_receptores)
    em['Subject'] = asunto
    em.set_content(mensaje)
    
    contexto = ssl.create_default_context()
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
        smtp.login(email_emisor, email_contraseña)
        smtp.sendmail(email_emisor, email_receptores, em.as_string())



    def obtener_datos():


        pass


#https://www.argentina.gob.ar/trabajo/seguridadsocial/ripte

#https://datos.gob.ar/dataset/sspm-salario-minimo-vital-movil-pesos-corrientes/archivo/sspm_57.1

