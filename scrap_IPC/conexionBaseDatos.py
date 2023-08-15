import mysql
import mysql.connector
import datetime
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd

class conexionBaseDatos:
    def cargaBaseDatos(self, lista_fechas, lista_region, lista_categoria, lista_division, lista_subdivision, lista_valores, host, user, password, database):
        try:
            conn = mysql.connector.connect(
                host=host, user=user, password=password, database=database
            )
            cursor = conn.cursor()
            
            lista_valores[20343]=271.5
            lista_valores[20344]=275.9
            lista_valores[20345]=281.2
            lista_valores[20346]=288.8
            
            df = pd.DataFrame()        
            df['fecha'] = lista_fechas
            df['region'] = lista_region
            df['categoria'] = lista_categoria
            df['division']= lista_division
            df['subdivision']= lista_subdivision
            df['valor'] = lista_valores
            
           # Sentencia SQL para comprobar si la fecha ya existe en la tabla
            select_query = "SELECT COUNT(*) FROM ipc_region WHERE Fecha = %s AND ID_Region = %s AND ID_Categoria = %s AND ID_Division = %s AND ID_Subdivision = %s"

            # Sentencia SQL para insertar los datos en la tabla
            insert_query = "INSERT INTO ipc_region (Fecha, ID_Region, ID_Categoria, ID_Division, ID_Subdivision, Valor) VALUES (%s, %s, %s, %s, %s, %s)"

            # Sentencia SQL para actualizar los datos en la tabla
            update_query = "UPDATE ipc_region SET Valor = %s WHERE Fecha = %s AND ID_Region = %s AND ID_Categoria = %s AND ID_Division = %s AND ID_Subdivision = %s"
   
            #Verificar cantidad de filas anteriores 
            select_row_count_query = "SELECT COUNT(*) FROM ipc_region"
            cursor.execute(select_row_count_query)
            row_count_before = cursor.fetchone()[0]
            
            for fecha, region, categoria, division, subdivision, valor in zip(lista_fechas, lista_region, lista_categoria, lista_division, lista_subdivision, lista_valores):
                # Convertir la fecha en formato datetime si es necesario
                if isinstance(fecha, str):
                    fecha = datetime.datetime.strptime(fecha, '%Y-%m-%d').date()

                # Verificar si el registro ya existe en la tabla
                cursor.execute(select_query, (fecha, region, categoria, division, subdivision))
                row_count = cursor.fetchone()[0]

                if row_count > 0:
                    # El registro ya existe, realizar una actualización (UPDATE)
                    cursor.execute(update_query, (valor, fecha, region, categoria, division, subdivision))
                    print("Leyendo el valor de IPC: ", valor)

                else:
                    # El registro no existe, realizar una inserción (INSERT)
                    cursor.execute(insert_query, (fecha, region, categoria, division, subdivision, valor))
                    print("Leyendo el valor de IPC: ", valor)

            #Obtener cantidad de filas
            cursor.execute(select_row_count_query)
            row_count_after = cursor.fetchone()[0]
            #Comparar la cantidad de antes y despues
            if row_count_after > row_count_before:
                print("Se agregaron nuevos datos")
                enviar_correo()   
            else:
                print("Se realizo una verificacion de la base de datos")
            
            print("antes:", row_count_before)
            print("despues:", row_count_after)
            
            # Confirmar los cambios en la base de datos
            conn.commit()

            # Cerrar el cursor y la conexión
            cursor.close()
            conn.close()

        except Exception as e:
            
            print(e)   

def enviar_correo():
    email_emisor = 'departamientoactualizaciondato@gmail.com'
    email_contraseña = 'oxadnhkcyjnyibao'
    email_receptores = ['gastongrillo2001@gmail.com', 'matizalazar2001@gmail.com','boscojfrancisco@gmail.com']
    asunto = 'Modificación en la base de datos'
    mensaje = 'Se ha producido una modificación en la base de datos de IPC.\n\nValores nuevos:\n'
    
    em = EmailMessage()
    em['From'] = email_emisor
    em['To'] = ", ".join(email_receptores)
    em['Subject'] = asunto
    em.set_content(mensaje)
    
    contexto = ssl.create_default_context()
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
        smtp.login(email_emisor, email_contraseña)
        smtp.sendmail(email_emisor, email_receptores, em.as_string())