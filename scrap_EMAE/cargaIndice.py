import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import mysql
import mysql.connector
from dateutil.relativedelta import relativedelta
from email.message import EmailMessage
import ssl
import smtplib

class cargaIndice:
    def loadXLSIndiceEMAE(self, file_path, lista_fechas, lista_SectorProductivo, lista_valores, host, user, password, database):

        try:

            # Leer el archivo Excel en un DataFrame de pandas
            df = pd.read_excel(file_path, sheet_name=0, skiprows=1)  # Leer el archivo XLSX y crear el DataFrame
            df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None
            # Eliminar la última fila que contiene "Fuente: INDEC"
            df = df.drop(df.index[-1])
            df = df.drop(df.index[-1])
            

            # Obtener las columnas desde C hasta R
            columnas_valores = df.columns[2:18]  # Columnas C a R
            
            fecha_inicio = datetime(2003, 12, 1)
            num_meses = len(df) - 2  # Restar 2 para compensar las filas de encabezados

            lista_fechas = [fecha_inicio + relativedelta(months=i) for i in range(num_meses)]

            # Iterar a través de las filas a partir de la fila 3
            for index, row in df.iterrows():
                if index >= 2:  # Fila 3 en adelante
                    fecha = lista_fechas[index - 2]  # Restar 2 para compensar el índice
                    for columna in columnas_valores:
                        valor = df.at[index, columna]
                        lista_valores.append(valor)
                        
                        sector_productivo = df.at[0, columna]  # Fila 1 (Fila de los años)
                        lista_SectorProductivo.append(sector_productivo)
                        print(f"Fecha: {fecha}, Valor: {valor}, Sector Productivo: {sector_productivo}")
            
            
            # Conexión a la base de datos MySQL
            conn = mysql.connector.connect(
                host=host, user=user, password=password, database=database
            )
            cursor = conn.cursor()

            #Verificar cantidad de filas anteriores 
            select_row_count_query = "SELECT COUNT(*) FROM emae"
            cursor.execute(select_row_count_query)
            row_count_before = cursor.fetchone()[0]
            
            delete_query ="TRUNCATE `prueba1`.`emae`"
            cursor.execute(delete_query)
            
            # Iterar a través de las filas a partir de la fila 3
            for index, row in df.iterrows():
                if index >= 3:  # Fila 3 en adelante
                    fecha = lista_fechas[index - 2]  # Restar 2 para compensar el índice
                    for columna in columnas_valores:
                        valor = df.at[index, columna]
                        sector_productivo = df.at[0, columna]  # Fila 1 (Fila de los años)
                        
                        # Insertar en la tabla MySQL
                        query = "INSERT INTO emae (Fecha, Sector_Productivo, Valor) VALUES (%s, %s, %s)"
                        values = (fecha, sector_productivo, valor)
                        cursor.execute(query, values)
            
            cursor.execute(select_row_count_query)
            row_count_after = cursor.fetchone()[0]
            #Comparar la cantidad de antes y despues
            
            if row_count_after > row_count_before:
                print("Se agregaron nuevos datos")
                enviar_correo()   
            else:
                print("Se realizo una verificacion de la base de datos")
                
            
            conn.commit()
            # Cerrar la conexión a la base de datos
            cursor.close()
            conn.close()
            
        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")

def enviar_correo():
    email_emisor='departamientoactualizaciondato@gmail.com'
    email_contraseña = 'oxadnhkcyjnyibao'
    email_receptor = ['gastongrillo2001@gmail.com', 'matizalazar2001@gmail.com','boscojfrancisco@gmail.com']
    asunto = 'Modificación en la base de datos'
    mensaje = 'Se ha producido una modificación en la base de datos.La tabla de EMAE contiene nuevos datos'
    
    em = EmailMessage()
    em['From'] = email_emisor
    em['To'] = email_receptor
    em['Subject'] = asunto
    em.set_content(mensaje)
    
    contexto= ssl.create_default_context()
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
        smtp.login(email_emisor, email_contraseña)
        smtp.sendmail(email_emisor, email_receptor, em.as_string())

