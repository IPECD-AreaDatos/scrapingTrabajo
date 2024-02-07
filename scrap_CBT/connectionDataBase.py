import os
import pandas as pd
#from sqlalchemy import create_engine
from email.message import EmailMessage
import ssl
import smtplib
import calendar
import xlrd
from datetime import datetime
import pymysql

class connection_db:

    def __init__(self, ssh_host, ssh_user, ssh_pem_key_path, mysql_host,mysql_port, mysql_user, mysql_password, system_operative ,database):
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.ssh_pem_key_path = ssh_pem_key_path
        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.system_operative = system_operative
        self.database = database
        self.tunel = None
        self.conn = None
        self.cursor = None


    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE A LAS CONEXIONES ==== #
    # =========================================================================================== #

    #Objetivo: Distiguir la conexion por SSH o por localhost
    def connect_db(self):

        #Si trabajamos sobre windows es necesario crear un tunel y conectar a la base de datos de forma remota
        if self.system_operative == 'Windows':
            
            #En caso de utilizar windows es necesario la importacion de 'sshtunel'
            from sshtunnel import SSHTunnelForwarder

            # ==== CONFIGURACION DE SSH

            print("* CONEXION - SSH INICADA")
            self.tunel = SSHTunnelForwarder(
                (self.ssh_host, 22), #--> Usando la IP publica, y el puerto 22
                ssh_username=self.ssh_user, #--> El definido para entrar al servidor
                ssh_pkey=self.ssh_pem_key_path,  # Ruta al archivo .pem
                remote_bind_address=('127.0.0.1', 3306) #--> Al puerto que nos conectamos de la EC2 es al 3306, de por medio usando el 22.
            )

            # Iniciar el túnel SSH
            self.tunel.start()
            print("* CONEXION - SSH EN FUNCIONAMIENTO")


            # ==== CONFIGURACION DE LA BASE DE DATOS

            print("CONEXION - BASE DE DATOS")
            # Conectar a MySQL a través del túnel SSH
            self.conn = pymysql.connect(
                host=self.mysql_host,
                user=self.mysql_user,
                password=self.mysql_password,
                database=self.database,
                port = self.tunel.local_bind_port
            )

            self.cursor = self.conn.cursor()
            print("CONEXION - BASE DE DATOS EN FUNCIONAMIENTO")
            print("SE HA CONECTADO UTILIZANDO UN SISTEMA OPERATIVO WINDOWS")



        #El caso de linux es usada para el servidor - No es necesario crear un tunel. solo conectar a la BDD.
        else:
            self.conn = pymysql.connect(
                host = '10.1.11.13',
                user = self.mysql_user,
                password = self.mysql_password,
                database = self.database
            )

            self.cursor = self.conn.cursor()
            print("SE HA CONECTADO UTILIZANDO UN SISTEMA OPERATIVO LINUX")

    def close_conections(self):

        # Confirmar los cambios en la base de datos y cerramos conexiones
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

        #Cerrar conexion con tunel
        self.tunel.close()


# =========================================================================================== #
                # ==== SECCION CORRESPONDIENTE AL DATALAKE ==== #
# =========================================================================================== #
        


    #Objetivo: Almacenar los datos de CBA y CBT sin procesar en el datalake. Datos sin procesar
    def load_datalake(self,df):
        
        #Obtenemos los tamanios
        tamanio_df,tamanio_bdd = self.determinar_tamanios(df)

        if tamanio_df > tamanio_bdd: #Si el DF es mayor que lo almacenado, cargar los datos nuevos
            
            #Obtengo diferencia de filas a cargar - En el nuevo dataframe solo estaran los datos nuevos
            df_datos_nuevos = df.tail(tamanio_df - tamanio_bdd)
            self.cargar_tabla_datalake(df_datos_nuevos)
            print("==== SE CARGARON DATOS NUEVOS CORRESPONDIENTES A CBT Y CBA DEL DATALAKE ====")
            
        else: #Si no hay datos nuevos AVISAR
            
            print("==== NO HAY DATOS NUEVOS CORRESPONDIENTES A CBT Y CBA DEL DATALAKE ====")     

    #Objetivo: Obtener tamaños de los datos para realizar verificaciones de varga
    def determinar_tamanios(self,df):

        #Obtenemos la cantidad de datos almacenados
        query_consulta = "SELECT COUNT(*) FROM cbt_cba"
        self.cursor.execute(query_consulta) #Ejecutamos la consulta
        tamanio_bdd = self.cursor.fetchone()[0] #Obtenemos el tamaño de la bdd
        
        #Obtenemos la cantidad de datos del dataframe construido
        tamanio_df = len(df)
        
        print(tamanio_bdd,tamanio_df)

        return tamanio_df,tamanio_bdd
    
    #Objetivo: almacenar en la tabla cbt_cba con los datos nuevos
    def cargar_tabla_datalake(self,df_cargar):

        query_insertar_datos = "INSERT INTO cbt_cba VALUES (%s, %s, %s, %s, %s,%s, %s)"

        for index,row in df_cargar.iterrows():
            
            #Obtenemos los valores de cada fila del DF
            values = (row['Fecha'],row['CBA_Adulto'],row['CBT_Adulto'],row['CBA_Hogar'],row['CBT_Hogar'],row['cba_nea'],row['cbt_nea'])


            # Convertir valores NaN a None --> Lo hacemos porque los valores 'nan' no son reconocidos por MYSQL
            values = [None if pd.isna(v) else v for v in values]

            #Realizamos carga
            self.cursor.execute(query_insertar_datos,values)

        #Cerramos conexiones
        self.close_conections()


# =========================================================================================== #        
# =========================================================================================== #

    """
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
        self.conn.close()"""

    def enviar_correo(self):


        cba_individuo,cbt_individuo,familia_indigente,familia_indigente_mes_anterior,familia_pobre,familia_pobre_mes_anterior,var_mensual_cba,var_mensual_cbt,var_interanual_cba,var_interanual_cbt,fecha = self.persona_individual_familia()

        mensaje_variaciones_nea = self.variaciones(5)
        mensaje_variaciones_CBTCBA = self.mensaje_variacion_interanual()
        #Cadenas de fecha para mostrar en el mensaje
        fecha_formato_normal = self.obtener_ultimafecha_actual(fecha)
        cba_mes_anterior = str(fecha.year)+"-"+str(fecha.month - 1)
        cba_mes_anterior = datetime.strptime(cba_mes_anterior, "%Y-%m")
        fecha_anterior_formato_normal = self.obtener_ultimafecha_actual(cba_mes_anterior)
        fecha_ano_anterior_formato_normal = self.obtener_ultimafecha_anoAnterior(fecha)

        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contrasenia = 'cmxddbshnjqfehka'
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com', 'samaniego18@gmail.com', 'guillermobenasulin@gmail.com', 'leclerc.mauricio@gmail.com']
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com']
        #-------------------------------- Mensaje nuevo --------------------------------
        asunto_wpp = f'CBA Y CBT - Actualización - Fecha: {fecha_formato_normal}'
        mensaje_wpp = f""" 

        <html> 
        <body>


        <h2> Datos correspondientes al Nordeste Argentino(NEA) </h2>

        
        <br>

        <p> Este correo contiene información respecto a <b>CBA</b> (Canasta Básica Alimentaria) y <b>CBT</b>(Canasta Básica Total).  </p>

        <hr>

        <p>
        🗓️En <span style="font-weight: bold;">{fecha_formato_normal}</span>, en el NEA una persona necesitó
        <span style="font-size: 17px; font-weight: bold;">${cba_individuo:,.2f}</span> para no ser
        <b>indigente</b> y
        <span style="font-size: 17px; font-weight: bold;">${cbt_individuo:,.2f}</span> para no ser
        <b>pobre</b>
        </p>

        <hr>

        <p> 👥👥Una familia tipo (compuesta por 4 integrantes) necesitó de 
        <span style="font-size: 17px;"><b>${familia_indigente:,.2f}</b></span> para no ser indigente y
        <span style="font-size: 17px;"><b>${familia_pobre:,.2f}</b></span> para no ser pobre. En {fecha_anterior_formato_normal}, una
        misma familia había necesitado 
        <span style="font-size: 17px;"><b>${familia_indigente_mes_anterior:,.2f}</b></span> para no ser indigente y 
        <span style="font-size: 17px;"><b>${familia_pobre_mes_anterior:,.2f}</b></span> para no ser pobre.
        </p> 

        <hr>
        {mensaje_variaciones_CBTCBA}
        <hr>

        {mensaje_variaciones_nea} <hr>

        <p> Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes<br>
            Dirección: Tucumán 1164 - Corrientes Capital<br>
            Contacto Coordinación General: 3794 284993</p>


        </body>
        </html>

        """

        mensaje_concatenado = mensaje_wpp
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = asunto_wpp
        em.set_content(mensaje_concatenado, subtype = 'html')
        
        contexto = ssl.create_default_context()
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contrasenia)
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

        #=== Variaciones Interanual
        ultima_fecha = df_bdd['fecha'].max()
        anio_anterior = ultima_fecha.year - 1
        mes= ultima_fecha.month

        valor_dic_anio_anterior_cba = df_bdd['CBA_nea'][ (df_bdd['fecha'].dt.year == anio_anterior) & (df_bdd['fecha'].dt.month == mes) ].values[0]
        valor_dic_anio_anterior_cbt = df_bdd['CBT_nea'][ (df_bdd['fecha'].dt.year == anio_anterior) & (df_bdd['fecha'].dt.month == mes) ].values[0]

        var_interanual_cba = ((cba_individuo /  valor_dic_anio_anterior_cba) - 1) * 100
        var_interanual_cbt = ((cbt_individuo / valor_dic_anio_anterior_cbt) - 1) * 100

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
        cba_nea_anio_anterior = df_bdd['CBA_nea'].iloc[-13] 
        cbt_nea_anio_anterior = df_bdd['CBT_nea'].iloc[-13]
                
        
        fecha = max(df_bdd['fecha'])



        return cba_individuo,cbt_individuo,familia_indigente,familia_indigente_mes_anterior,familia_pobre,familia_pobre_mes_anterior,var_mensual_cba,var_mensual_cbt,var_interanual_cba,var_interanual_cbt,ultima_fecha
    
    def obtener_ultimafecha_anoAnterior(self,fecha_ultimo_registro):
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

        return f"{nombre_mes_espanol} del {fecha_ultimo_registro.year-1}"

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

    def variaciones(self,region):

        nombre_tabla = 'ipc_region'

        query_consulta = f'SELECT * FROM {nombre_tabla} WHERE ID_Region = {region} and ID_Categoria = 1'

        #query_prueba = 'SELECT * FROM ipc_region WHERE ID_Region = 1 and ID_Categoria = 1'
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

        grupo_mes_actual_anio_anterior= df_bdd[(df_bdd['Fecha'].dt.year == fecha_max.year-1 ) & (df_bdd['Fecha'].dt.month == fecha_max.month)]

        total_ipc_anio_anterior =  grupo_mes_actual_anio_anterior['Valor'].values[0]

        variacion_interanual = ((total_ipc / total_ipc_anio_anterior) - 1) * 100



        #=== CALCULO VARIACION ACUMULADA - Variacion desde DIC del año anterior

        grupo_diciembre_anio_anterior = df_bdd[ (df_bdd['Fecha'].dt.year == fecha_max.year-1 ) & ((df_bdd['Fecha'].dt.month == 12)) ]

        total_diciembre = grupo_diciembre_anio_anterior['Valor'].values[0]

        variacion_acumulada = ((total_ipc / total_diciembre) - 1) * 100

        #Var mensual 
        parte_entera_mensual, parte_decimal_mensual = str(variacion_mensual).split('.')
        numero_truncado_mensual = '.'.join([parte_entera_mensual, parte_decimal_mensual[:1]])
        

        
        #Var interanual 
        parte_entera_interanual, parte_decimal_interanual = str(variacion_interanual).split('.')
        numero_truncado_interanual = '.'.join([parte_entera_interanual, parte_decimal_interanual[:1]])


        
        #Var Acumulada
        parte_entera_acumulada, parte_decimal_acumuludad = str(variacion_acumulada).split('.')
        numero_truncado_acumulado = '.'.join([parte_entera_acumulada, parte_decimal_acumuludad[:1]])
        

        
        if region == 5: #--> Region NEA
            #DATOS SACOS DEL EXCEL DIRECTAMENTE
            numero_truncado_mensual, numero_truncado_interanual = self.var_mensual_prueba()

        cba_individuo,cbt_individuo,familia_indigente,familia_indigente_mes_anterior,familia_pobre,familia_pobre_mes_anterior,var_mensual_cba,var_mensual_cbt,var_interanual_cba,var_interanual_cbt,fecha = self.persona_individual_familia()
        fecha_formato_normal = self.obtener_ultimafecha_actual(fecha)
        cba_mes_anterior = str(fecha.year)+"-"+str(fecha.month - 1)
        cba_mes_anterior = datetime.strptime(cba_mes_anterior, "%Y-%m")
        fecha_anterior_formato_normal = self.obtener_ultimafecha_actual(cba_mes_anterior)


        cadena_variaciones =f"""
        
        <p>
        📈Respecto al Índice de Precios al Consumidor del NEA, para el mes de {fecha_formato_normal} la variación general de precios respecto al mes anterior fue de 
        <span style="font-size: 17px;"><b>{numero_truncado_mensual}%</b></span>. La variación interanual fue de <span style="font-size: 17px;"><b>{numero_truncado_interanual}%</b></span>
        ({fecha_formato_normal} vs {fecha_anterior_formato_normal})
        </p>
        """

        return cadena_variaciones
    
    def mensaje_variacion_interanual(self):
        cba_individuo,cbt_individuo,familia_indigente,familia_indigente_mes_anterior,familia_pobre,familia_pobre_mes_anterior,var_mensual_cba,var_mensual_cbt,var_interanual_cba,var_interanual_cbt,fecha = self.persona_individual_familia()
        fecha_formato_normal = self.obtener_ultimafecha_actual(fecha)
        cba_mes_anterior = str(fecha.year)+"-"+str(fecha.month - 1)
        cba_mes_anterior = datetime.strptime(cba_mes_anterior, "%Y-%m")
        fecha_anterior_formato_normal = self.obtener_ultimafecha_actual(cba_mes_anterior)
        fecha_ano_anterior_formato_normal = self.obtener_ultimafecha_anoAnterior(fecha)

        # Verificar si var_interanual_cba es negativa o positiva
        if var_interanual_cba < 0:
            aumento_disminucion = "disminuyó⬇️"
        else:
            aumento_disminucion = "aumentó⬆️"
        
        if var_interanual_cbt < 0:
            aumento_canastaBasica = "disminuyó⬇️"
        else:
            aumento_canastaBasica = "aumentó⬆️"

        if var_mensual_cba < 0:
            aumento_mensual_canastaBasica= "disminuyó⬇️"
        else:
            aumento_mensual_canastaBasica = "aumentó⬆️"

        if var_mensual_cbt < 0:
            aumento_mensual_canastaTotal=  "disminuyó⬇️"
        else:
            aumento_mensual_canastaTotal = "aumentó⬆️"
        
        cadena_variaciones = f"""
        <p>
        La canasta básica alimentaria {aumento_disminucion} interanualmente ({fecha_formato_normal} vs {fecha_ano_anterior_formato_normal}) un 
        <span style="font-size: 17px; font-weight: bold;">{abs(var_interanual_cba):.2f}%</span>,
        mientras que la canasta básica total {aumento_canastaBasica} para el mismo periodo un 
        <span style="font-size: 17px; font-weight: bold;">{var_interanual_cbt:.2f}%</span>.
        </p>
        <p>
        La canasta básica alimentaria {aumento_mensual_canastaBasica} mensualmente ({fecha_formato_normal} vs {fecha_anterior_formato_normal}) un 
        <span style="font-size: 17px; font-weight: bold;">{var_mensual_cba:.2f}%</span>
        mientras que la canasta básica total {aumento_mensual_canastaTotal} para el mismo periodo un 
        <span style="font-size: 17px; font-weight: bold;">{var_mensual_cbt:.2f}%</span>.
        </p>
        """
        return cadena_variaciones
    
    
    def var_mensual_prueba(self):
        #Construccion de la direccion
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        nuevo_directorio = os.path.join(directorio_desagregado, "..")
        nuevo_directorio = os.path.abspath(nuevo_directorio)
        ruta_carpeta_files = os.path.join(nuevo_directorio, 'Scrap_IPC')
        ruta_carpeta_files = os.path.join(ruta_carpeta_files, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'IPC_Desagregado.xls')
        print("aca ", file_path_desagregado)
            
        # Leer el archivo de xls y obtener la hoja de trabajo específica
        workbook = xlrd.open_workbook(file_path_desagregado)
        sheet = workbook.sheet_by_index(0)  # Hoja 1 (índice 0)


        target_row_index = 154
        #Fila de variaciones mensuales del NEA
        ultima_var_mensual = sheet.row_values(target_row_index + 3)[-1]



        #Fila de variaciones interanuales del NEA
        sheet = workbook.sheet_by_index(1)  # Hoja 2 (índice 1)

        fechas = sheet.row_values(target_row_index + 2)
        ult_var_interanual = sheet.row_values(target_row_index + 2)[-1]

        return ultima_var_mensual, ult_var_interanual

