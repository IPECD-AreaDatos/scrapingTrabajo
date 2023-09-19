import mysql
import mysql.connector
import datetime
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd
from armadoXLSDataNacion import LoadXLSDataNacion

class conexionBaseDatos:

    def __init__(self,lista_fechas, lista_region, lista_categoria, lista_division, lista_subdivision, lista_valores, host, user, password, database):

        self.lista_fechas = lista_fechas
        self.lista_region = lista_region
        self.lista_categoria = lista_categoria
        self.lista_division = lista_division
        self.lista_subdivision = lista_subdivision
        self.lista_valores = lista_valores
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def conectar_bdd(self):

            self.conn = mysql.connector.connect(
                host=self.host, user=self.user, password=self.password, database=self.database
            )
            self.cursor = self.conn.cursor()


    def cargaBaseDatos(self):
        
        try:
            self.conectar_bdd(self)
            
            df = pd.DataFrame()        
            df['fecha'] = self.lista_fechas
            df['region'] = self.lista_region
            df['categoria'] = self.lista_categoria
            df['division']= self.lista_division
            df['subdivision']= self.lista_subdivision
            df['valor'] = self.lista_valores
            
           # Sentencia SQL para comprobar si la fecha ya existe en la tabla
            select_query = "SELECT COUNT(*) FROM ipc_region WHERE Fecha = %s AND ID_Region = %s AND ID_Categoria = %s AND ID_Division = %s AND ID_Subdivision = %s"

            # Sentencia SQL para insertar los datos en la tabla
            insert_query = "INSERT INTO ipc_region (Fecha, ID_Region, ID_Categoria, ID_Division, ID_Subdivision, Valor) VALUES (%s, %s, %s, %s, %s, %s)"

            #Verificar cantidad de filas anteriores 
            select_row_count_query = "SELECT COUNT(*) FROM ipc_region"
            self.cursor.execute(select_row_count_query)
            row_count_before = self.cursor.fetchone()[0]
            
            delete_query ="TRUNCATE `prueba1`.`ipc_region`"
            self.cursor.execute(delete_query)

            for fecha, region, categoria, division, subdivision, valor in zip(self.lista_fechas, self.lista_region, self.lista_categoria, self.lista_division, self.lista_subdivision, self.lista_valores):
                # Convertir la fecha en formato datetime si es necesario
                if isinstance(fecha, str):
                    fecha = datetime.datetime.strptime(fecha, '%Y-%m-%d').date()

                self.cursor.execute(insert_query, (fecha, region, categoria, division, subdivision, valor))
                print("Leyendo el valor de IPC: ", valor)


            self.verificar_cantidad(self,row_count_before)


            # Confirmar los cambios en la base de datos
            self.conn.commit()

            # Cerrar el cursor y la conexión
            self.cursor.close()
            self.conn.close()

            LoadXLSDataNacion().loadInDataBase(self.host, self.user, self.password, self.database)
            
            
        except Exception as e:
            
            print(e)   


    def enviar_correo(self):
        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        email_receptores = ['gastongrillo2001@gmail.com', 'matizalazar2001@gmail.com', 'boscojfrancisco@gmail.com']
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


    def verificar_cantidad(self,row_count_before):
       
        #Verificar cantidad de filas anteriores 
        select_row_count_query = "SELECT COUNT(*) FROM ipc_region"
        #Obtener cantidad de filas
        self.cursor.execute(select_row_count_query)
        row_count_after = self.cursor.fetchone()[0]

        #Comparar la cantidad de antes y despues
        if row_count_after > row_count_before:
            print("Se agregaron nuevos datos")
            self.enviar_correo()   
        else:
            print("Se realizo una verificacion de la base de datos")
        
        print("antes:", row_count_before)
        print("despues:", row_count_after)


    #Objetivo: calcular la variacion mensual, intearanual y acumulado del IPC a nivel nacional
    def variaciones(self,region):

        nombre_tabla = 'ipc_region'
        query_consulta = f'SELECT * FROM {nombre_tabla} WHERE ID_Region = {region} and ID_Subdivision = 1'
        df_bdd = pd.read_sql(query_consulta,self.conn)

        #==== IPC registrado en el AÑO Y MES actual ==== #

        #Obtener ultima fecha
        fecha_max = df_bdd['Fecha'].max()

        #Buscamos los registros de la ultima fecha - Sumamos todos los campos
        grupo_ultima_fecha = df_bdd[(df_bdd['Fecha'] == fecha_max)]

        total_ipc = grupo_ultima_fecha['Valor'].values[0]

         #=== CALCULO DE LA VARIACION MENSUAL

        #Buscamos el mes anterior
        mes_anterior = int(fecha_max.month) - 1

        #Convertimos la serie a datetime para buscar por año y mes
        df_bdd['Fecha'] = pd.to_datetime(df_bdd['Fecha'])    
        grupo_mes_anterior = df_bdd[ (df_bdd['Fecha'].dt.year == fecha_max.year) & (df_bdd['Fecha'].dt.month == mes_anterior)]

        #Total del mes anterior y calculo de la variacion mensual
        total_mes_anterior = grupo_mes_anterior['Valor'].values[0]

        variacion_mensual = ((total_ipc/ total_mes_anterior) - 1) * 100


        #=== CALCULO VARIACION INTERANUAL

        grupo_mes_actual_año_anterior= df_bdd[(df_bdd['Fecha'].dt.year == fecha_max.year-1 ) & (df_bdd['Fecha'].dt.month == fecha_max.month)]

        total_ipc_año_anterior =  grupo_mes_actual_año_anterior['Valor'].values[0]

        variacion_interanual = ((total_ipc / total_ipc_año_anterior) - 1) * 100


        #=== CALCULO VARIACION ACUMULADA - Variacion desde DIC del año anterior

        grupo_diciembre_año_anterior = df_bdd[ (df_bdd['Fecha'].dt.year == fecha_max.year-1 ) & ((df_bdd['Fecha'].dt.month == 12)) ]

        total_diciembre = grupo_diciembre_año_anterior['Valor'].values[0]

        variacion_acumulada = ((total_ipc / total_diciembre) - 1) * 100
        
        print("* variacion mensual:",variacion_mensual)
        print("* Variacion INTERANUAL",variacion_interanual)
        print("* Variacion ACUMULADA",variacion_acumulada)

    #Objetivo: calcular las variaciones mensuales de las subdiviones del NEA
    def vars_mensual_nea(self):

        nombre_tabla = 'ipc_region'
        query_consulta = f'SELECT * FROM {nombre_tabla} WHERE ID_Region = 5'
        df_bdd = pd.read_sql(query_consulta,self.conn)  

        #Obtener ultima fecha
        fecha_max = df_bdd['Fecha'].max()

        #Buscamos los registros de la ultima fecha 
        grupo_ultima_fecha = df_bdd[(df_bdd['Fecha'] == fecha_max)]


        #Buscamos el mes anterior
        mes_anterior = int(fecha_max.month) - 1

        #Convertimos la serie a datetime para buscar por año y mes
        df_bdd['Fecha'] = pd.to_datetime(df_bdd['Fecha'])    
        grupo_mes_anterior = df_bdd[ (df_bdd['Fecha'].dt.year == fecha_max.year) & (df_bdd['Fecha'].dt.month == mes_anterior)]
        
        
        #Vamos a detectar los valores de cada subdivision y calcular su variacion mensual
        #Luego lo agregaremos a una tabla de STR o HTML para trabajar el conjunto en su totalidad

        for indice in grupo_ultima_fecha['ID_Subdivision']:
            

            #Busqueda del valor del mes anterior
            fila_mes_anterior = grupo_mes_anterior[grupo_mes_anterior['ID_Subdivision'] == indice]
            valor_mes_anterior = fila_mes_anterior['Valor']

            #Valor del mes actual
            valor_mes_actual = grupo_ultima_fecha[grupo_ultima_fecha['ID_Subdivision'] == indice]

            print(valor_mes_actual)


        
        




#SECCION DE PRUEBAS

#Listas a tratar durante el proceso
lista_fechas = list()
lista_region = list()
lista_categoria = list()
lista_division = list()
lista_subdivision = list()
lista_valores = list()

#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

instancia = conexionBaseDatos(lista_fechas, lista_region, lista_categoria, lista_division, lista_subdivision, lista_valores, host, user, password, database)
instancia.conectar_bdd()

"""
instancia.variaciones(1) #--> Nacion
print("\n ---------  \n")
instancia.variaciones(5) #--> NEA
"""

instancia.vars_mensual_nea()