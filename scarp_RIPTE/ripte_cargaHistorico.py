import mysql.connector
import numpy as np
import pandas as pd
import time
import os
from email.message import EmailMessage
import ssl
import smtplib

nuevos_datos = []
    
class ripte_cargaHistorico:
    def loadInDataBase(self, host, user, password, database):
        #Se toma el tiempo de comienzo
        start_time = time.time()
        
        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )

        # Nombre de la tabla en MySQL
        table_name = 'ripte'
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        # Construir la ruta de la carpeta "files" dentro del directorio actual
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_name = "ripte_historico.csv"
        # Construir la ruta completa del archivo CSV dentro de la carpeta "files"
        file_path = os.path.join(ruta_carpeta_files, file_name)
                
        # Leer el archivo de csv y hacer transformaciones
        df = pd.read_csv(file_path)  # Leer el archivo CSV y crear el DataFrame
        df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

        print("columnas -- ", df.columns)
        print(df)
        
        print("Ripte")
        insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in range(len(df.columns))])})"
        update_query = f"UPDATE {table_name} SET RIPTE = %s WHERE Fecha = %s "

        for index, row in df.iterrows():
            data_tuple = tuple(row)
            conn.cursor().execute(insert_query, data_tuple)
            print("Inserción realizada:", data_tuple)
            
            # Agregar los datos nuevos a la lista
            nuevos_datos.append(data_tuple)

        conn.commit()
            
        print("====================================================================")
        print("Se realizo la actualizacion de la tabla de Salarios Sector Privado")
        print("====================================================================")

        #Se toma el tiempo de finalizacion y se calcula
        end_time = time.time()
        duration = end_time - start_time
        print(f"Tiempo de ejecución: {duration} segundos")
        
        # Cerrar la conexión a la base de datos
        conn.close()
        
        
def enviar_correo():
    email_emisor='departamientoactualizaciondato@gmail.com'
    email_contraseña = 'oxadnhkcyjnyibao'
    email_receptor = ['boscojfrancisco@gmail.com','gastongrillo2001@gmail.com']
    asunto = 'Modificación en la base de datos'
    mensaje = 'Se ha producido una modificación en la base de datos.'
    body = "Se han agregado nuevos datos:\n\n"
    for data in nuevos_datos:
        body += ', '.join(map(str, data)) + '\n'
    
    em = EmailMessage()
    em['From'] = email_emisor
    em['To'] = email_receptor
    em['Subject'] = asunto
    em.set_content(mensaje)
    
    contexto= ssl.create_default_context()
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
        smtp.login(email_emisor, email_contraseña)
        smtp.sendmail(email_emisor, email_receptor, em.as_string())