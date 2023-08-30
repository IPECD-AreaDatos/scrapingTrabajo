import mysql
import mysql.connector
import datetime
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd
import sqlalchemy

class conexionBaseDatos:

    def __init__(self,host, user, password, database):

        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
        
    #Conectamos a la BDD y configuramos 'conn' y 'cursor'
    def conectar_bdd(self):
            
        try:

            self.conn = mysql.connector.connect(
                host=self.host, user= self.user, password=self.password, database=self.database
            )
            self.cursor = self.conn.cursor() #--cursor para usar BDD
        

        except Exception as e:

            print("ERROR:",e)


    #Cargamos los datos a la tabla 'salario_mvm' - Tambien detecta datos nuevos para ir cargandolos
    def cargar_datos(self,df):


        #Revisamos posiblidad de actualizacion
        try: 

            #Usaremos estos datos para ver que datos hay que agregar especificamente
            cantidad_fila_df,filas_df_bdd,bandera = self.revisar_tablas(df)

        except Exception as e:

            bandera = False

        if bandera:

            #tail toma las ultima n filas. En este caso (filas de df - filas de bdd) = filas a cargar
            df_aux = df.tail(cantidad_fila_df - filas_df_bdd)

            nombre_tabla = 'salario_mvm'

            #Query de insercion
            query_insercion = f"INSERT INTO {nombre_tabla} (fecha, salario_mvm_mensual, salario_mvm_diario, salario_mvm_hora) VALUES (%s, %s, %s,%s)"

            for fecha,mvm_mensual,mvm_dia,mvm_hora in zip(df_aux['indice_tiempo'],df_aux['salario_minimo_vital_movil_mensual'],df_aux['salario_minimo_vital_movil_diario'],df_aux['salario_minimo_vital_movil_hora']):
                
                #Ejecucion del query creado recientemente
                self.cursor.execute(query_insercion,(fecha,mvm_mensual,mvm_dia,mvm_hora))

            self.conn.commit()  # Hacer commit después de todas las inserciones
            self.cursor.close()
            self.conn.close()

            print("- SE HA PRODUCIDO UNA ACTUALIZACION DE DATOS")
            self.enviar_correo()
        else:

            print("- No existen datos nuevos para cargar.")


    #Revisaremos el numero de filas para revisar si hay datos nuevos - Lo haremos comprobando el df extraido de la web, y el df extraido de la bdd
    def revisar_tablas(self,df):

            self.conectar_bdd()

            #Buscamos los datos de la BDD
            nombre_tabla = 'salario_mvm'
            query_carga = f"SELECT * FROM {nombre_tabla}"
            df_bdd = pd.read_sql(query_carga,self.conn)


            #Revisamos si poseen la misma cantidad de filas
            filas_df_bdd = len(df_bdd)
            filas_df = len(df)

            if filas_df_bdd < filas_df: #Se producira la carga
                 
                return filas_df,filas_df_bdd,True
            
            else: #No se producira la carga
                return False
                

                    

    def enviar_correo(self):
        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        email_receptor = ['boscojfrancisco@gmail.com','gastongrillo2001@gmail.com']
        asunto = 'Modificación en la base de datos'
        mensaje = 'Se ha producido una modificación en la base de datos. La tabla de SALARIO MINIMO VITAL Y MOVIL contiene nuevos datos.'
        
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptor)
        em['Subject'] = asunto
        em.set_content(mensaje)
        
        contexto = ssl.create_default_context()
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptor, em.as_string())