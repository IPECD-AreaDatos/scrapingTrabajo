import os
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
from email.message import EmailMessage
import ssl
import smtplib
import calendar


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


        cba_individuo,cbt_individuo,familia_indigente,familia_indigente_mes_anterior,familia_pobre,familia_pobre_mes_anterior,var_mensual_cba,var_mensual_cbt,var_interanual_cba,var_interanual_cbt,fecha = self.persona_individual_familia()



        
        #Cadenas de fecha para mostrar en el mensaje
        fecha_formato_normal = self.obtener_ultimafecha_actual(fecha)
        cadena_fecha = str(fecha.year)+"-"+str(fecha.month)
        cba_mes_anterior = str(fecha.year)+"-"+str(fecha.month - 1)

        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com']
        #email_receptores =  ['benitezeliogaston@gmail.com']
        asunto = f'CBA Y CBT - Actualizacion - Fecha: {fecha_formato_normal}'

        mensaje_1 = f""" 

        <html> 
        <body>


        <h2> Datos correspondientes al Nordeste Argentino(NEA) </h2>

        
        <br>

        <p> Este correo contiene informacion respeto al <b>CBA</b> (Canasta Basica Alimnentaria) y <b>CBT</b>(Canasta Basica Total).  </p>

        <hr>

        <h3> Para una persona individual, a la fecha: </h3>

        <p>Se necesito <span style="font-size: 17px;"><b>${cba_individuo:,.2f}</b></span> para no ser indigente.</p>

        <p>Se necesito <span style="font-size: 17px;"><b>${cbt_individuo:,.2f}</b></span> para no ser pobre</p>

        <hr>

        <h3>En la fecha {cadena_fecha}, para una familia compuesto por 4 integrantes: </h3>

        <p>Se necesito <span style="font-size: 17px;"><b>${familia_indigente:,.2f}</b></span> para no ser indigente.</p>
        <p>Se necesito <span style="font-size: 17px;"><b>${familia_pobre:,.2f}</b></span> para no ser pobre.</p>

        <h3> El mes anterior {cba_mes_anterior}, para una familia compuesta por 4 integrantes: 

        <p>Se necesito <span style="font-size: 17px;"><b>${familia_indigente_mes_anterior:,.2f}</b></span> para no ser indigente.</p>
        <p>Se necesito <span style="font-size: 17px;"><b>${familia_pobre_mes_anterior:,.2f}</b></span> para no ser pobre.</p>

        <hr>

       <h3> Variaciones Interanuales: </h3>

        <p>CBA tuvo una variacion interanual de: <span style="font-size: 17px;"><b>{var_interanual_cba:.2f}%</b></span></p>

        <p>CBT tuvo una variacion interanual de: <span style="font-size: 17px;"><b>{var_interanual_cbt:.2f}%</b></span></p>


        <h3> Variaciones Mensuales: </h3>

        <p>CBA tuvo una variacion mensual de: <span style="font-size: 17px;"><b>{var_mensual_cba:.2f}%</b></span></p>
        <p>CBT tuvo una variacion mensual de: <span style="font-size: 17px;"><b>{var_mensual_cbt:.2f}%</b></span></p>


        
        <hr>

        <p> Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes<br>
            Dirección: Tucumán 1164 - Corrientes Capital<br>
            Contacto Coordinación General: 3794 284993</p>


        </body>
        </html>

        """

        
        mensaje_concatenado = mensaje_1
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = asunto
        em.set_content(mensaje_concatenado, subtype = 'html')
        
        contexto = ssl.create_default_context()
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())


    #Objetivo: calcular el valor para que una  persona individual o una familia, no sea pobre o no sea indigente.
    #PERSONA INDIVIDUAL --> Correspondencia con CBA || FAMILIA DE 4 PERSONAS --> Correspondiente al calculo de 
    def persona_individual_familia(self):

        #Construccion de consulta y obtencion del dataframe
        nombre_tabla = 'canasta_basica'
        consulta = f'SELECT * FROM {nombre_tabla}'
        df_bdd = pd.read_sql(consulta,self.conn)
        df_bdd['fecha'] = pd.to_datetime(df_bdd['fecha'])

        
        #Obtencion de datos INDIVIDUALES POR PERSONA de CBA y CBT
        cba_individuo = df_bdd['CBA_nea'].values[-1]
        cbt_individuo = df_bdd['CBT_nea'].values[-1]

        #Calculo de familia para no ser indigente y para no ser pobre
        familia_indigente = cba_individuo * 3.09
        familia_pobre = cbt_individuo * 3.09


        # === DATOS DEL MES ANTERIOR DE NACION === #
        cba_mes_anterior = df_bdd['CBA_nea'].iloc[-2]
        cbt_mes_anterior = df_bdd['CBT_nea'].iloc[-2]


        #Calculo de familia para no ser indigente y para no ser pobre
        familia_indigente_mes_anterior = cba_mes_anterior * 3.09
        familia_pobre_mes_anterior = cbt_mes_anterior * 3.09




        # ==== VARIACIONES ==== #

        #=== Variaciones Mensuales
        var_mensual_cba = ((cba_individuo / cba_mes_anterior) - 1) * 100  #--> indigente
        var_mensual_cbt =  ((cbt_individuo / cbt_mes_anterior) -1) * 100 #--> pobre

        #=== Variaciones Intearanual
        ultima_fecha = df_bdd['fecha'].max()
        año_anterior = ultima_fecha.year - 1
        mes= ultima_fecha.month

        valor_dic_año_anterior_cba = df_bdd['CBA_nea'][ (df_bdd['fecha'].dt.year == año_anterior) & (df_bdd['fecha'].dt.month == mes) ].values[0]
        valor_dic_año_anterior_cbt = df_bdd['CBT_nea'][ (df_bdd['fecha'].dt.year == año_anterior) & (df_bdd['fecha'].dt.month == mes) ].values[0]

        var_interanual_cba = ((cba_individuo /  valor_dic_año_anterior_cba) - 1) * 100
        var_interanual_cbt = ((cbt_individuo / valor_dic_año_anterior_cbt) - 1) * 100

        print(f"""
        
        *CBT Individuo: {cba_individuo}
        *CBA Individuo: {cbt_individuo}
        *Familia Indigente: {familia_indigente} - Mes anterior: {familia_indigente_mes_anterior}
        *Familia Pobre: {familia_pobre} - Mes anterior: {familia_pobre_mes_anterior}
        *Var. Mensual CBA: {var_mensual_cba}
        *Var. Mensual CBT: {var_mensual_cbt}
        *Var. Interanual CBA: {var_interanual_cba}
        *Var. Interanual CBT: {var_interanual_cbt}
        """)


        # === DATOS DEL AÑO ANTERIOR NEA === #

        #--> Hacemos -13 por un tema de indices, el dato que buscamos es el mismo de la fecha pero del año anterior (osea 12 meses atras)
        #La bdd toma un valor atrasado
        cba_nea_año_anterior = df_bdd['CBA_nea'].iloc[-13] 
        cbt_nea_año_anterior = df_bdd['CBT_nea'].iloc[-13]
                
        
        fecha = max(df_bdd['fecha'])

        return cba_individuo,cbt_individuo,familia_indigente,familia_indigente_mes_anterior,familia_pobre,familia_pobre_mes_anterior,var_mensual_cba,var_mensual_cbt,var_interanual_cba,var_interanual_cbt,ultima_fecha
    
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

        return str(fecha_ultimo_registro.day) + f" de {nombre_mes_espanol} del {fecha_ultimo_registro.year}"



"""
host = '172.17.22.23'
user = 'team-datos'
password = 'HCj_BmbCtTuCv5}'
database = 'ipecd_economico'

instancia = connection_db(host,user,password,database)
instancia.conectar_bdd()
instancia.persona_individual_familia()
"""
