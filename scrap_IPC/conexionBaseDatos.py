import mysql
import mysql.connector
import datetime
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd
from armadoXLSDataNacion import LoadXLSDataNacion

class conexionBaseDatos:
    def cargaBaseDatos(self, lista_fechas, lista_region, lista_categoria, lista_division, lista_subdivision, lista_valores, host, user, password, database):
        try:
            conn = mysql.connector.connect(
                host=host, user=user, password=password, database=database
            )
            cursor = conn.cursor()
            
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

            #Verificar cantidad de filas anteriores 
            select_row_count_query = "SELECT COUNT(*) FROM ipc_region"
            cursor.execute(select_row_count_query)
            row_count_before = cursor.fetchone()[0]
            
            delete_query ="TRUNCATE `prueba1`.`ipc_region`"
            cursor.execute(delete_query)

            for fecha, region, categoria, division, subdivision, valor in zip(lista_fechas, lista_region, lista_categoria, lista_division, lista_subdivision, lista_valores):
                # Convertir la fecha en formato datetime si es necesario
                if isinstance(fecha, str):
                    fecha = datetime.datetime.strptime(fecha, '%Y-%m-%d').date()

                cursor.execute(insert_query, (fecha, region, categoria, division, subdivision, valor))
                print("Leyendo el valor de IPC: ", valor)


            # Confirmar los cambios en la base de datos
            conn.commit()

            # Cerrar el cursor y la conexión
            cursor.close()
            conn.close()

            LoadXLSDataNacion().loadInDataBase(host, user, password, database)
            
            verificar_cantidad(host, user, password, database, row_count_before)
            
        except Exception as e:
            
            print(e)   

def enviar_correo():
    email_emisor = 'departamientoactualizaciondato@gmail.com'
    email_contraseña = 'oxadnhkcyjnyibao'
    email_receptores = ['gastongrillo2001@gmail.com', 'matizalazar2001@gmail.com', 'sebadikow@gmail.com']
    asunto = 'Modificación en la base de datos'
    mensaje = 'Se ha producido una modificación en la base de datos. La tabla de IPC contiene nuevos datos'
    
    em = EmailMessage()
    em['From'] = email_emisor
    em['To'] = ", ".join(email_receptores)
    em['Subject'] = asunto
    em.set_content(mensaje)
    
    contexto = ssl.create_default_context()
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
        smtp.login(email_emisor, email_contraseña)
        smtp.sendmail(email_emisor, email_receptores, em.as_string())

def verificar_cantidad(host, user, password, database, row_count_before):
    conn = mysql.connector.connect(
        host=host, user=user, password=password, database=database
    )
    cursor = conn.cursor()
    
    #Verificar cantidad de filas anteriores 
    select_row_count_query = "SELECT COUNT(*) FROM ipc_region"
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
    
    # Cerrar el cursor y la conexión
    cursor.close()
    conn.close()



