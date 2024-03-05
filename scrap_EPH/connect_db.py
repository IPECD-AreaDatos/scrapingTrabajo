import mysql.connector
import pandas as pd
from datetime import datetime
import calendar
from email.message import EmailMessage
import ssl
import smtplib
from pandas import isna

class connect_db:
    def connect(self, df, host, user, password, database):
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        cursor = conn.cursor()

        table_name= 'eph_tasas'
        delete_query = f"TRUNCATE {database}.{table_name}"
        query_cantidad_datos = f'SELECT COUNT(*) FROM {table_name}'
        query_insertar_datos = f"INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s, %s, %s)"
        
        #Obtencion de tamaño del DF sacado del EXCEL
        cant_filas_df = len(df)
        #Obtencion de cantidad de filas de 
        cursor.execute(query_cantidad_datos)
        row_count_before = cursor.fetchone()[0]

        if (cant_filas_df > row_count_before):
            #Eliminamos tabla
            cursor.execute(delete_query)

            #Iteramos el dataframe, y vamos cargando fila por fila
            for index,valor in df.iterrows():
                values = (valor['Aglomerado'], valor['Año'], valor['Fecha'], valor['Trimestre'], valor['Tasa de Actividad'], valor['Tasa de Empleo'], valor['Tasa de desocupación'])
                # Convertir valores NaN a None --> Lo hacemos porque los valores 'nan' no son reconocidos por MYSQL
                values = [None if isna(v) else v for v in values]
                
                #Insercion de dato fila por fila
                cursor.execute(query_insertar_datos,values)
            conn.commit()
            cursor.close()
            conn.close()
        else:
            print(f"NO HAY CAMBIOS EN LOS DATOS DE {table_name}")

    def envio_correo(self, df_datos_nuevos): 
        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com']
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com']
        fecha = df_datos_nuevos["Fecha"].iloc[-1]  # Accede a la última fecha desde el DataFrame
        fecha_arreglada =self.obtener_ultimafecha_actual(fecha)
        asunto = f'ACTUALIZACION - IPICORR - {fecha_arreglada}'
        mensaje = f'''\
            <html>
            <body>
            <h2>Se ha producido una modificación en la base de datos. La tabla de IPICORR contiene nuevos datos.</h2>
            <p>*Variacion Interanual IPICORR: <span style="font-size: 17px;"><b>{df_datos_nuevos["Var_Interanual_IPICORR"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual Alimentos: <span style="font-size: 17px;"><b>{df_datos_nuevos["Var_Interanual_Alimentos"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual Textil: <span style="font-size: 17px;"><b>{df_datos_nuevos["Var_Interanual_Textil"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual Maderas: <span style="font-size: 17px;"><b>{df_datos_nuevos["Var_Interanual_Maderas"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual min. No Metalicos: <span style="font-size: 17px;"><b>{df_datos_nuevos["Var_Interanual_MinNoMetalicos"].iloc[-1]}</b></span></p>
            <hr>
            <p>*Variacion Interanual Metales: <span style="font-size: 17px;"><b>{df_datos_nuevos["Var_Interanual_Metales"].iloc[-1]}</b></span></p>
            <hr>
            <p> Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes<br>
            Dirección: Tucumán 1164 - Corrientes Capital<br>
            Contacto Coordinación General: 3794 284993</p>
            </body>
            </html>
                    '''
                
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = asunto
        em.set_content(mensaje, subtype = 'html')
                
        contexto = ssl.create_default_context()
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())


    def obtener_ultimafecha_actual(self,fecha_ultimo_registro):
        
        # Obtener el nombre del mes actual en inglés
        nombre_mes_ingles = calendar.month_name[fecha_ultimo_registro.month]

        # Diccionario de traducción
        traducciones_meses = {
            'January': 'Enero',
            'February': 'Febrero',
            'March': 'Marzo',
            'April': 'Abril',
            'May': 'Mayo',
            'June': 'Junio',
            'July': 'Julio',
            'August': 'Agosto',
            'September': 'Septiembre',
            'October': 'Octubre',
            'November': 'Noviembre',
            'December': 'Diciembre'
        }

        # Obtener la traducción
        nombre_mes_espanol = traducciones_meses.get(nombre_mes_ingles, nombre_mes_ingles)

        return f"{nombre_mes_espanol} del {fecha_ultimo_registro.year}"
    
