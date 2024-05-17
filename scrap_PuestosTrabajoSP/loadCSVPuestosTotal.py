import mysql.connector
import numpy as np
import pandas as pd
import time
import os
from email.message import EmailMessage
import ssl
import smtplib

nuevos_datos = []

class LoadCSVDataPuestosTotal:
    def loadInDataBase(self, host, user, password, database):
        start_time = time.time()
    
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        cursor = conn.cursor()
        
        # Nombre de la tabla en MySQL
        table_name = 'dp_puestostrabajo_total'
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_name = "trabajoTotal.csv"
        # Construir la ruta completa del archivo CSV dentro de la carpeta "files"
        file_path = os.path.join(ruta_carpeta_files, file_name)
                
        # Leer el archivo de csv y hacer transformaciones
        df = pd.read_csv(file_path)  # Leer el archivo CSV y crear el DataFrame
        df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None
        longitud_datos_excel = len(df)
        print("total: ", longitud_datos_excel)

        select_row_count_query = "SELECT COUNT(*) FROM dp_puestostrabajo_total"
        cursor.execute(select_row_count_query)
        filas_BD = cursor.fetchone()[0]
        print("Base: ", filas_BD)

        if longitud_datos_excel != filas_BD:
            df_datos_nuevos = df.tail(longitud_datos_excel - filas_BD)
            
            # Aplicar strip() al nombre de la columna antes de acceder a ella
            column_name = ' puestos '  # Nombre de la columna con espacios en blanco
            column_name_stripped = column_name.strip()  # Eliminar espacios en blanco

            # Verificar si la columna existe en el DataFrame
            if column_name_stripped in df_datos_nuevos.columns:
                df_datos_nuevos.loc[df_datos_nuevos[column_name_stripped] < 0, column_name_stripped] = 0 #Los datos <0 se reempalazan a 0
            else:
                print(f"La columna '{column_name_stripped}' no existe en el DataFrame.")
            
            
            print("Tabla de Puestos Trabajos Total")
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in range(len(df_datos_nuevos.columns))])})"
            for index, row in df_datos_nuevos.iterrows():
                data_tuple = tuple(row)
                conn.cursor().execute(insert_query, data_tuple)
                print(data_tuple)
                nuevos_datos.append(data_tuple)
            conn.commit()
            conn.close()
            print("Se agregaron nuevos datos")
            enviar_correo()   
        else:
            print("Se realizo una verificacion de la base de datos")
        
        print("====================================================================")
        print("Se realizo la actualizacion de la Tabla de Puestos Trabajos Total")
        print("====================================================================")
        
def enviar_correo():
    email_emisor='departamientoactualizaciondato@gmail.com'
    email_contraseña = 'cmxddbshnjqfehka'
    email_receptor = ['matizalazar2001@gmail.com','gastongrillo2001@gmail.com']
    asunto = 'Modificación en la base de datos'
    mensaje = 'Se ha producido una modificación en la base de datos.Tabla de Puestos Trabajos Total'
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