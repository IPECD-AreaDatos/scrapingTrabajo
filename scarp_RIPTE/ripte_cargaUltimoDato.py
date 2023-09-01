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
import pandas as pd
from datetime import datetime

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
            self.conn.commit()

            print("Se agregaron nuevos datos")
            self.enviar_correo(nueva_fecha, contenido_float, ultimo_ripte)  # Pasar los valores a la función

        cursor.close()
        self.conn.close()
            
            
            
    def enviar_correo(self,nueva_fecha, nuevo_valor, valor_anterior):  # Recibir los valores como argumentos


        #Obtencion de valores para informe por CORREO
        variacion_mensual = ((nuevo_valor / valor_anterior) - 1) * 100
        variacion_interanual, variacion_acumulada, fecha_mes_anterior,fecha_mes_AñoAnterior, diciembre_AñoAnterior = self.obtener_datos(nueva_fecha,nuevo_valor)
        nueva_fecha = nueva_fecha.date()

        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com']
        

        asunto = f'Modificación en la base de datos - Remuneración Imponible Promedio de los Trabajadores Estables (RIPTE) - Fecha {nueva_fecha}'
        mensaje = f'''\
            <html>
            <body>
            <h2>Se ha producido una modificación en la base de datos de RIPTE.</h2>
            <hr>
            <p>Nueva fecha: {nueva_fecha} -- Nuevo valor:  <span style="font-size: 17px;"><b>${nuevo_valor}<b></p>
            <hr>
            <p>Valor correspondiente a {fecha_mes_anterior}: ${valor_anterior} -- Variación Mensual:  <span style="font-size: 17px;"><b>{variacion_mensual:.2f}%</b>  </p>
            <hr>
            <p>Variación interanual de {nueva_fecha} a {fecha_mes_AñoAnterior}:  <span style="font-size: 17px;"><b>{variacion_interanual:.2f}%</b> </p>
            <hr>
            <p>Variación Acumulada desde {diciembre_AñoAnterior} a {nueva_fecha}:  <span style="font-size: 17px;"><b>{variacion_acumulada:.2f}%</b> </p>
            </body>

            
            Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes <br>
            Dirección: Tucumán 1164 - Corrientes Capital
            - Contacto Coordinación General: 3794 284993
        
            </html>
            '''
        
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = asunto
        em.set_content(mensaje, subtype = 'html')
        
        contexto = ssl.create_default_context()
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())


    #Objetivo 
    def obtener_datos(self,nueva_fecha,nuevo_valor):

        #Obtencion de datos de la BDD - Transformacion a DF
        nombre_tabla = 'ripte'
        consulta = f'SELECT * FROM {nombre_tabla}'
        df_bdd = pd.read_sql(consulta,self.conn)


        # ==== CALCULO VARIACION INTERANUAL ==== #


        #Obtencion de la fecha actual - se usara para determinar el valor del año anterior en el mismo mes
        fecha_actual = df_bdd['Fecha'].iloc[-1]

        año_actual = fecha_actual.year
        año_anterior = año_actual - 1

        mes_actual = str(fecha_actual.month)
        dia_actual = str(fecha_actual.day)

        #Construccion de la fecha y busqueda del valor
        fecha_mes_AñoAnterior = str(año_anterior)+"-"+mes_actual+"-"+dia_actual
        fecha_mes_AñoAnterior = datetime.strptime(fecha_mes_AñoAnterior,'%Y-%m-%d').date()

        valor_mes_AñoAnterior = df_bdd.loc[df_bdd['Fecha'] == fecha_mes_AñoAnterior]
        print(valor_mes_AñoAnterior)
        valor = valor_mes_AñoAnterior['ripte'].values[0]

        print(f'NUEVO VALOR:{nuevo_valor} - VALOR ANTERIOR: {valor}')

        #Calculo final
        variacion_interanual = ((nuevo_valor / valor) - 1 ) * 100


        # ===== CALCULO VARIACION ACUMULADA ==== #

        diciembre_AñoAnterior = datetime.strptime(str(año_anterior) + "-" + "12-01",'%Y-%m-%d').date() #--> Fecha de DIC del año anterior

        #SMVM del año anterior
        valor_dic_AñoAnterior = df_bdd.loc[df_bdd['Fecha'] == diciembre_AñoAnterior]
        smvm_dic_AñoAnterior = valor_dic_AñoAnterior['ripte'].values[0]

        #calculo final
        variacion_acumulada = ((nuevo_valor / smvm_dic_AñoAnterior) - 1) * 100




        return variacion_interanual, variacion_acumulada,fecha_mes_AñoAnterior,fecha_mes_AñoAnterior,diciembre_AñoAnterior

#https://www.argentina.gob.ar/trabajo/seguridadsocial/ripte

#https://datos.gob.ar/dataset/sspm-salario-minimo-vital-movil-pesos-corrientes/archivo/sspm_57.1

