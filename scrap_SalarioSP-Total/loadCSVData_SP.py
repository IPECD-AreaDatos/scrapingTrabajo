from email.message import EmailMessage
import ssl
import smtplib
import pymysql
from sqlalchemy import create_engine

nuevos_datos = []
    
class Gestion_bdd:

    def __init__(self,host, user, password, database):

        self.host = host
        self.user = user
        self.password =password
        self.database = database
        self.conn = None
        self.cursor = None

    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE A LAS CONEXIONES ==== #
    # =========================================================================================== #        


    #Objetivo: conectar a la base de datos
    def connect_db(self):
            
            #Creamos conexion, y cursor de la conexion
            self.conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
            self.cursor = self.conn.cursor()   

    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE AL DATALAKE ==== #
    # =========================================================================================== #


    #Objetivo: Obtenemos las cantidades de fila del DF extraido, y el de la BDD. Si hay diferencia entonces se cargara
    def contador_filas(self,df,table):

        #Tamano del DF original
        tamano_df = len(df)

        #Tamaño del DF de la BASE
        select_row_count_query = f"SELECT COUNT(*) FROM {table}"
        self.cursor.execute(select_row_count_query)
        filas_bdd = self.cursor.fetchone()[0]

        return tamano_df, filas_bdd


    def loadInDataBase(self,df,table):
        

        #Nos conectamos a la BDD
        self.connect_db()    

        #Obtencion de tamaños
        len_df, len_bdd = self.contador_filas(df,table)

        #Si el tamaño del DF del excel, es mayor al que esta en la BDD,entonces realizar carga.
        if len_df > len_bdd:

            #Obtenemos SOLO LOS DATOS A CARGAR, por diferencia de filas
            df_datalake = df.tail(len_df-len_bdd)

            #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
            engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}") #--> Conector
            df_datalake.to_sql(name=f"{table}", con=engine, if_exists='append', index=False) #--> Carga de tabla de salarios del sector privado


            #Guardamos cambios 
            self.conn.commit()

        else:
            print("No existen nuevos datos para cargar.")
            
        return
    

        
        if longitud_datos_excel > filas_BD:
            df_datos_nuevos = df.tail(longitud_datos_excel - filas_BD)

            column_name = ' w_mean ' 
            column_name_stripped = column_name.strip() 

            # Verificar si la columna existe en el DataFrame
            if column_name_stripped in df_datos_nuevos.columns:
                df_datos_nuevos.loc[df_datos_nuevos[column_name_stripped] < 0, column_name_stripped] = 0 #Los datos <0 se reempalazan a 0
            else:
                print(f"La columna '{column_name_stripped}' no existe en el DataFrame.")
            
            print("Tabla de Salarios Sector Privado")
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
        print("Se realizo la actualizacion de la tabla de Salarios Sector Privado")
        print("====================================================================")
        
        
def enviar_correo():
    email_emisor='departamientoactualizaciondato@gmail.com'
    email_contraseña = 'cmxddbshnjqfehka'
    email_receptor = ['matizalazar2001@gmail.com','benitezeliogaston@gmail.com']
    asunto = 'Modificación en la base de datos'
    mensaje = 'Se ha producido una modificación en la base de datos.Tabla de Salarios Sector Privado'
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