import mysql.connector
import pandas as pd
from datetime import datetime
import calendar
from email.message import EmailMessage
import ssl
import smtplib


class Correo_ipi_nacion():


    def __init__(self):
        
        self.cursor = None
        self.conn = None

    #Conectar con la BDD
    def connect(self, host, user, password, database):

        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        self.cursor = self.conn.cursor()

    #Construccion del correo
    def construccion_correo(self):
        
        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com']

        table_name = 'ipi'
        query_consulta = f'SELECT * FROM {table_name} ORDER BY fecha DESC LIMIT 1'
        df_bdd = pd.read_sql(query_consulta,self.conn)

        asunto = f'ACTUALIZACION - Índice de producción industrial manufacturero(IPI) - {df_bdd["fecha"].iloc[-1]}'

        mensaje = f'''\
            <html>
            <body>
            <h2>Se ha producido una modificación en la base de datos. La tabla de IPI contiene nuevos datos.</h2>
            <p>*Variacion Interanual IPI: <span style="font-size: 17px;"><b>{df_bdd["var_IPI"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual Alimentos: <span style="font-size: 17px;"><b>{df_bdd["var_interanual_alimentos"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual Textil: <span style="font-size: 17px;"><b>{df_bdd["var_interanual_textil"].iloc[-1]}</b></span></p>
            <hr>
            <hr>
            <p>*Variacion Interanual Sustancias: <span style="font-size: 17px;"><b>{df_bdd["var_interanual_sustancias"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual Maderas: <span style="font-size: 17px;"><b>{df_bdd["var_interanual_maderas"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual min. No Metalicos: <span style="font-size: 17px;"><b>{df_bdd["var_interanual_MinNoMetalicos"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual Metales: <span style="font-size: 17px;"><b>{df_bdd["var_interanual_metales"].iloc[-1]}</b></span></p>
            <hr>
            <p> Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes<br>
            Dirección: Tucumán 1164 - Corrientes Capital<br>
            Contacto Coordinación General: 3794 284993</p>
            </body>
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


#Datos de la base de datos
host = '172.17.22.23'
user = 'team-datos'
password = 'HCj_BmbCtTuCv5}'
database = 'ipecd_economico'

instancia = Correo_ipi_nacion()
instancia.connect(host, user, password, database)
instancia.construccion_correo()
