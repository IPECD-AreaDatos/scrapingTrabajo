import pandas as pd
from email.message import EmailMessage
import ssl
import smtplib



class MailCBTCBA():


    #Objetivo: extraer los datos correspondientes al correo de CBT y CBA de la tabla A1
    def extract_date(self):

        query = "SELECT * FROM correo_CBT_CBA"
        df = pd.read_sql()


    #Objetivo: enviar los correos de CBT y CBA. Le pasamos como parametro 'conn' que es la conexion a la BDD.
    def send_mail_cbt_cba(self,conn):


        #Extracion de datos





        #Declaramos email desde el que se envia, la contraseña de la api, y los correos receptores.
        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contrasenia = 'cmxddbshnjqfehka'

        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com', 'samaniego18@gmail.com', 'guillermobenasulin@gmail.com', 'leclerc.mauricio@gmail.com']
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com']

        #==== Zona de envio de correo
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = "Tema a definir"
        em.set_content("MENSAJE a definir", subtype = 'html')

        contexto = ssl.create_default_context()

        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contrasenia)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())