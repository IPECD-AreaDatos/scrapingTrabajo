import os
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from email.message import EmailMessage
import ssl
import smtplib

class connection_db:
    def carga_db(self, host, user, password, database):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path = os.path.join(ruta_carpeta_files, 'Calculos.xlsx')
        
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        cursor = connection.cursor()

        # Leer el archivo Excel
        df = pd.read_excel(file_path)

        # Función para intentar convertir a float y manejar errores
        def try_convert_float(value):
            try:
                return float(value)
            except (ValueError, TypeError):
                return None

        # Aplicar la función de conversión a columnas numéricas
        columnas_numericas = ['CBA_Adulto', 'CBT_Adulto', 'CBA_Hogar', 'CBT_Hogar']
        for columna in columnas_numericas:
            df[columna] = df[columna].apply(try_convert_float)

        # Función para intentar convertir a fecha y manejar errores
        def try_convert_date(value):
            try:
                return pd.to_datetime(value, errors='coerce').date()
            except (ValueError, TypeError):
                return None

        # Aplicar la función de conversión a la columna de fecha
        df['Fecha'] = df['Fecha'].apply(try_convert_date)
        
        # Nombre de la tabla en MySQL
        table_name = "canasta_basica"  # Reemplaza con el nombre de tu tabla en MySQL

        # Crear una cadena de conexión SQLAlchemy
        connection_string = f"mysql+mysqlconnector://{user}:{password}@{host}/{database}"

        # Crear una conexión a la base de datos utilizando SQLAlchemy
        engine = create_engine(connection_string)

        select_row_count_query = "SELECT COUNT(*) FROM Canasta_Basica"
        cursor.execute(select_row_count_query)
        row_count_before = cursor.fetchone()[0]
        
        delete_query ="TRUNCATE `ipecd_economico`.`Canasta_Basica`"
        cursor.execute(delete_query)

        # Cargar los datos en MySQL
        df.to_sql(table_name, engine, if_exists="append", index=False)
        
        cursor.execute(select_row_count_query)
        row_count_after = cursor.fetchone()[0]
        
        #Comparar la cantidad de antes y despues
        if row_count_after > row_count_before:
            print("Se agregaron nuevos datos")
            enviar_correo()   
        else:
            print("Se realizo una verificacion de la base de datos")
        

        # Cerrar la conexión a MySQL
        cursor.close()
        connection.close()

def enviar_correo():
    email_emisor='departamientoactualizaciondato@gmail.com'
    email_contraseña = 'cmxddbshnjqfehka'
    email_receptor = ['benitezeliogaston@gmail.com']
    asunto = 'Modificación en la base de datos'
    mensaje = 'Se ha producido una modificación en la base de datos.Tabla de Canasta Basica'
    body = "Se han agregado nuevos datos"

    
    em = EmailMessage()
    em['From'] = email_emisor
    em['To'] = email_receptor
    em['Subject'] = asunto
    em['body'] = body
    em.set_content(mensaje)
    
    contexto= ssl.create_default_context()
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
        smtp.login(email_emisor, email_contraseña)
        
        