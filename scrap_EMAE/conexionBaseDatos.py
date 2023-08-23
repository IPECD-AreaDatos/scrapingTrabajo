import mysql
import mysql.connector
import datetime
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd

class conexionBaseDatos:
    def cargaBaseDatos(self, lista_fechas, lista_SectorProductivo, lista_valores, host, user, password, database):
        try:
            # Conexión a la base de datos MySQL
            conn = mysql.connector.connect(
                host='tu_host',
                user='tu_usuario',
                password='tu_contraseña',
                database='tu_base_de_datos'
            )
            cursor = conn.cursor()

            # Iterar a través de las filas a partir de la fila 3
            for index, row in df.iterrows():
                if index >= 2:  # Fila 3 en adelante
                    fecha = lista_fechas[index - 2]  # Restar 2 para compensar el índice
                    for columna in columnas_valores:
                        valor = df.at[index, columna]
                        sector_productivo = df.at[0, columna]  # Fila 1 (Fila de los años)
                        
                        # Insertar en la tabla MySQL
                        query = "INSERT INTO emae (Fecha, Sector_Productivo, Valor) VALUES (%s, %s, %s)"
                        values = (fecha, sector_productivo, valor)
                        cursor.execute(query, values)
                        conn.commit()

            # Cerrar la conexión a la base de datos
            cursor.close()
            conn.close()

        except Exception as e:
            
            print(e)   

def enviar_correo():
    email_emisor = 'departamientoactualizaciondato@gmail.com'
    email_contraseña = 'oxadnhkcyjnyibao'
    email_receptores = ['gastongrillo2001@gmail.com', 'matizalazar2001@gmail.com','boscojfrancisco@gmail.com']
    asunto = 'Modificación en la base de datos'
    mensaje = 'Se ha producido una modificación en la base de datos de IPC.'
    
    em = EmailMessage()
    em['From'] = email_emisor
    em['To'] = ", ".join(email_receptores)
    em['Subject'] = asunto
    em.set_content(mensaje)
    
    contexto = ssl.create_default_context()
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
        smtp.login(email_emisor, email_contraseña)
        smtp.sendmail(email_emisor, email_receptores, em.as_string())