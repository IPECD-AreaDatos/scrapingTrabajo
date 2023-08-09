import mysql.connector
import numpy as np
import pandas as pd
import time
import os
from email.message import EmailMessage
import ssl
import smtplib

nuevos_datos = []
    
class loadCSVData_SP:
    def loadInDataBase(self, host, user, password, database):
        #Se toma el tiempo de comienzo
        start_time = time.time()
        
        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )

        # Nombre de la tabla en MySQL
        table_name = 'DP_salarios_sector_privado'
        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        # Construir la ruta de la carpeta "files" dentro del directorio actual
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_name = "salarioPromedioSP.csv"
        # Construir la ruta completa del archivo CSV dentro de la carpeta "files"
        file_path = os.path.join(ruta_carpeta_files, file_name)
                
        # Leer el archivo de csv y hacer transformaciones
        df = pd.read_csv(file_path)  # Leer el archivo CSV y crear el DataFrame
        df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

        print("columnas -- ", df.columns)

        # Aplicar strip() al nombre de la columna antes de acceder a ella
        column_name = ' w_mean '  # Nombre de la columna con espacios en blanco
        column_name_stripped = column_name.strip()  # Eliminar espacios en blanco

        # Verificar si la columna existe en el DataFrame
        if column_name_stripped in df.columns:
            # Realizar transformaciones en el DataFrame utilizando el nombre de columna sin espacios
            df.loc[df[column_name_stripped] < 0, column_name_stripped] = 0 #Los datos <0 se reempalazan a 0
        else:
            print(f"La columna '{column_name_stripped}' no existe en el DataFrame.")
        
        print("Tabla de Salarios Sector Privado")
        insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in range(len(df.columns))])})"
        update_query = f"UPDATE {table_name} SET salario = %s WHERE fecha = %s AND codigo_departamento_indec = %s AND id_provincia_indec = %s AND clae2 = %s"

        for index, row in df.iterrows():
            data_tuple = tuple(row)
            
            # Verificar si los valores ya existen en la tabla
            check_query = f"SELECT * FROM {table_name} WHERE fecha = %s AND codigo_departamento_indec = %s AND id_provincia_indec = %s AND clae2 = %s"
            check_data = (row['fecha'], row['codigo_departamento_indec'], row['id_provincia_indec'], row['clae2'])
            conn.cursor().execute(check_query, check_data)
            existing_data = conn.cursor().fetchone()

            if existing_data:
                # Si los valores ya existen, realizar la actualización
                update_data = (row['salario'], row['fecha'], row['codigo_departamento_indec'], row['id_provincia_indec'], row['clae2'])
                conn.cursor().execute(update_query, update_data)
                print("Actualización realizada:", update_data)
            else:
                # Si los valores no existen, realizar la inserción
                conn.cursor().execute(insert_query, data_tuple)
                print("Inserción realizada:", data_tuple)
                
                # Agregar los datos nuevos a la lista
                nuevos_datos.append(data_tuple)

        conn.commit()

        # Enviar correo solo si hay nuevos datos
        if nuevos_datos:
            enviar_correo()
            
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
    email_receptor = 'gastongrillo2001@gmail.com'
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