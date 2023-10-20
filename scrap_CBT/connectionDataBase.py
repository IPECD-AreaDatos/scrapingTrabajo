import os
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from email.message import EmailMessage
import ssl
import smtplib

class connection_db:

    def __init__(self,host, user, password, database):

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
    
    def carga_db(self):

        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path = os.path.join(ruta_carpeta_files, 'Calculos.xlsx')

        #Conectar la BDD
        self.conectar_bdd()
        

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
        connection_string = f"mysql+mysqlconnector://{self.user}:{self.password}@{self.host}/{self.database}"

        # Crear una conexión a la base de datos utilizando SQLAlchemy
        engine = create_engine(connection_string)

        select_row_count_query = "SELECT COUNT(*) FROM Canasta_Basica"
        self.cursor.execute(select_row_count_query)
        row_count_before = self.cursor.fetchone()[0]
        
        delete_query ="TRUNCATE `ipecd_economico`.`Canasta_Basica`"
        self.cursor.execute(delete_query)

        # Cargar los datos en MySQL
        df.to_sql(table_name, engine, if_exists="append", index=False)
        
        self.cursor.execute(select_row_count_query)
        row_count_after = self.cursor.fetchone()[0]
        
        #Comparar la cantidad de antes y despues
        if row_count_after > row_count_before:
            print("Se agregaron nuevos datos")
            self.enviar_correo()   
        else:
            print("Se realizo una verificacion de la base de datos")
        

        # Cerrar la conexión a MySQL
        self.cursor.close()
        self.conn.close()

    def enviar_correo(self):
        
        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        email_receptor = ['benitezeliogaston@gmail.com']
        asunto = 'Modificación en la base de datos'
        mensaje = 'Se ha producido una modificación en la base de datos.Tabla de Canasta Basica'

        
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptor)
        em['Subject'] = asunto
        em.set_content(mensaje, subtype = 'html')
        
        contexto = ssl.create_default_context()
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptor, em.as_string())


    #Objetivo: calcular el valor para que una  persona individual o una familia, no sea pobre o no sea indigente.
    def persona_individual(self):

        #Construccion de consulta y obtencion del dataframe
        nombre_tabla = 'canasta_basica'
        consulta = f'SELECT * FROM {nombre_tabla}'
        df_bdd = pd.read_sql(consulta,self.conn)
        df_bdd['fecha'] = pd.to_datetime(df_bdd['fecha'])

        #Buscamos la fecha maxima del NEA registrado
        no_nulos = df_bdd['CBA_nea'].dropna()
        
        #Buscamos los indices, y agarramos el ultimo --> Este numero de fila nos interesa para obtener la ultima fecha
        indice_ultimo_valor = no_nulos.index[-1]
        fecha_maxima_nea = df_bdd['fecha'].iloc[indice_ultimo_valor]

        #Buscamos el año de la fecha y obtenemos la lista de valores correspondiente al año
        año = int(fecha_maxima_nea.year)

        """ 
        Tomamos los ultimos 6 valores NO NULOS de la lista DEL NEA - TANTO CBA como CBT
        Hacemos los mismo para GBA pero los primeros 6 valores de cada lista.
        Tambien buscamos los ultimos valores de CBA y CBT de GBA
        """    
        lista_cba_nea = (df_bdd['CBA_nea'][df_bdd['fecha'].dt.year == año].dropna())[-6:]
        lista_cbt_nea = (df_bdd['CBT_nea'][df_bdd['fecha'].dt.year == año].dropna())[-6:]

        lista_cba_gba = (df_bdd['CBA_Adulto'][df_bdd['fecha'].dt.year == año].dropna())[:6]
        lista_cbt_gba =  (df_bdd['CBT_Adulto'][df_bdd['fecha'].dt.year == año].dropna())[:6]


        ultimo_cba = df_bdd['CBA_Adulto'].iloc[-1]
        ultimo_cbt = df_bdd['CBT_Adulto'].iloc[-1]

        #Calculos para no ser indigente y no ser pobre
        indigente = (sum(lista_cba_nea) / sum(lista_cba_gba)) * ultimo_cba
        pobre = (sum(lista_cbt_nea) / sum(lista_cbt_gba)) * ultimo_cbt

        #Calculo de familia para no ser indigente y para no ser pobre
        familia_indigente = indigente * 3.09
        familia_pobre = pobre * 3.09

        return indigente, pobre , familia_indigente, familia_pobre
        
    def variaciones(self):

        #Construccion de consulta y obtencion del dataframe
        nombre_tabla = 'canasta_basica'
        consulta = f'SELECT * FROM {nombre_tabla}'
        df_bdd = pd.read_sql(consulta,self.conn)
        df_bdd['fecha'] = pd.to_datetime(df_bdd['fecha'])

        #Ultima fecha
        fecha_max = max(df_bdd['fecha'])
        ano = fecha_max.year
        mes = fecha_max.month
    
        #Ultimos valores
        ultimo_cba_adulto = df_bdd['CBA_Adulto'].iloc[-1]
        ultimo_cbt_adulto = df_bdd['CBT_Adulto'].iloc[-1]
        
        #Valores del año anterior
        ultimo_cba_adulto_año_anterior =  df_bdd['CBA_Adulto'][(df_bdd['fecha'].dt.year == (ano-1)) & (df_bdd['fecha'].dt.month == (mes))].values[0]
        ultimo_cbt_adulto_año_anterior =  df_bdd['CBT_Adulto'][(df_bdd['fecha'].dt.year == (ano-1)) & (df_bdd['fecha'].dt.month == (mes))].values[0]

        var_inter_cba = ((ultimo_cba_adulto / ultimo_cba_adulto_año_anterior) - 1) * 100
        var_inter_cbt = ((ultimo_cbt_adulto / ultimo_cbt_adulto_año_anterior) - 1) * 100

        print(ultimo_cba_adulto,ultimo_cba_adulto_año_anterior)

        print(var_inter_cba,var_inter_cbt)

#SECCION DE PRUEBAS
host = '172.17.16.157'
user = 'team-datos'
password = 'HCj_BmbCtTuCv5}'
database = 'ipecd_economico'

instancia = connection_db(host,user,password,database)
instancia.conectar_bdd()
instancia.variaciones()

