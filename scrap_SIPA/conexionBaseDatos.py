import mysql
import mysql.connector
import datetime
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd

class conexionBaseDatos:
    def cargaBaseDatos(self, host, user, password, database, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas):
        try:
            conn = mysql.connector.connect(
                host=host, user=user, password=password, database=database
            )
            cursor = conn.cursor()
            
            #Se le asigna la lista correspondiente a la columna del data frame y se arma el "Excel"
            df = pd.DataFrame() 
            df['fecha'] = lista_fechas
            df['id_prov'] = lista_provincias
            df['tipo_registro'] = lista_registro
            df['valores_estacionales'] = lista_valores_estacionalidad
            df['valores_no_estacionales'] = lista_valores_sin_estacionalidad
            
            # Verificar cuantas filas tiene la tabla de mysql
            select_query = "SELECT COUNT(*) FROM sipa_registro WHERE Fecha = %s"
            
            # Sentencia SQL para insertar los datos en la tabla sipa_registro
            insert_query = "INSERT INTO sipa_registro (Fecha, ID_Provincia, ID_Tipo_Registro, Cantidad_con_Estacionalidad, Cantidad_sin_Estacionalidad) VALUES (%s, %s, %s, %s, %s)"

            # Sentencia SQL para actualizar los datos en la tabla
            update_query = "UPDATE sipa_registro SET Cantidad_con_Estacionalidad = %s, Cantidad_sin_Estacionalidad = %s WHERE Fecha = %s AND ID_Provincia = %s AND ID_Tipo_Registro = %s"

            #Verificar cantidad de filas anteriores 
            select_row_count_query = "SELECT COUNT(*) FROM sipa_registro"
            cursor.execute(select_row_count_query)
            row_count_before = cursor.fetchone()[0]
            
            delete_query ="TRUNCATE `prueba1`.`sipa_registro`"
            cursor.execute(delete_query)
            
            for fecha, id_prov, tipo_registro, valores_estacionales, valores_no_estacionales in zip(lista_fechas, lista_provincias, lista_registro, lista_valores_estacionalidad, lista_valores_sin_estacionalidad):
                # Convertir la fecha en formato datetime si es necesario
                if isinstance(fecha, str):
                    fecha = datetime.datetime.strptime(fecha, '%Y-%m-%d').date()

                cursor.execute(insert_query, (fecha, id_prov, tipo_registro, valores_estacionales, valores_no_estacionales))
                        
            #Obtener cantidad de filas
            cursor.execute(select_row_count_query)
            row_count_after = cursor.fetchone()[0]
            #Comparar la cantidad de antes y despues
            if row_count_after > row_count_before:
                print("Se agregaron nuevos datos")
                enviar_correo()   
            else:
                print("Se realizo una verificacion de la base de datos")
                
            # Confirmar los cambios en la base de datos
            conn.commit()
            # Cerrar el cursor y la conexión
            cursor.close()
            conn.close()

        except Exception as e:
            
            print(e)   

def enviar_correo():
    email_emisor='departamientoactualizaciondato@gmail.com'
    email_contraseña = 'oxadnhkcyjnyibao'
    email_receptor = 'gastongrillo2001@gmail.com, matizalazar2001@gmail.com'
    asunto = 'Modificación en la base de datos'
    mensaje = 'Se ha producido una modificación en la base de datos.La tabla de SIPA posee nuevos valores'
    
    em = EmailMessage()
    em['From'] = email_emisor
    em['To'] = email_receptor
    em['Subject'] = asunto
    em.set_content(mensaje)
    
    contexto= ssl.create_default_context()
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
        smtp.login(email_emisor, email_contraseña)
        smtp.sendmail(email_emisor, email_receptor, em.as_string())