from email.message import EmailMessage
import ssl
import smtplib
import numpy as np
import pymysql
from sqlalchemy import create_engine
import os
import pandas as pd

nuevos_datos = []

class LoadCSVDataPuestosTotal:
    def __init__(self,host, user, password, database):

        self.host = host
        self.user = user
        self.password =password
        self.database = database
        self.conn = None
        self.cursor = None

    def connect_db(self):
            
            #Creamos conexion, y cursor de la conexion
            self.conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
            self.cursor = self.conn.cursor()   


    def contador_filas(self,df,table):

        #Tamano del DF original
        tamano_df = len(df)

        #Tamaño del DF de la BASE
        select_row_count_query = f"SELECT COUNT(*) FROM {table}"
        self.cursor.execute(select_row_count_query)
        filas_bdd = self.cursor.fetchone()[0]

        return tamano_df, filas_bdd
    
    def loadInDataBase(self):
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
        
#Nos conectamos a la BDD
        self.connect_db()    

        #Obtencion de tamaños
        len_df, len_bdd = self.contador_filas(df,table_name)

        longitud_datos_excel = len(df)
        print("privado: ", longitud_datos_excel)
        
        if len_df > len_bdd:

            #Obtenemos SOLO LOS DATOS A CARGAR, por diferencia de filas
            df_datalake = df.tail(len_df-len_bdd)

            #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}") #--> Conector
            df_datalake.to_sql(name=f"{table_name}", con=engine, if_exists='append', index=False) #--> Carga de tabla de salarios del sector privado


            #Guardamos cambios 
            self.conn.commit()

        else:
            print("No existen nuevos datos para cargar.")
            
        return
    
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