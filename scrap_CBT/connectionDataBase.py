import os
import pandas as pd
from sqlalchemy import create_engine
from email.message import EmailMessage
import ssl
import smtplib
import calendar
import xlrd
from datetime import datetime
import pymysql
from sqlalchemy import create_engine

class connection_db:

    def __init__(self,mysql_host,mysql_user, mysql_password ,database):

        self.mysql_host = mysql_host
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.database = database
        self.tunel = None
        self.conn = None
        self.cursor = None

    # =========================================================================================== #
                    # ==== SECCION CORRESPONDIENTE A SETTERS Y GETTERS ==== #
    # =========================================================================================== #
            
    #Objetivo: cambiar el nombre de la base de datos para reconectarnos a otra.
    def set_database(self,new_name):

        self.database = new_name


    # =========================================================================================== #
            # ==== SECCION CORRESPONDIENTE A LAS CONEXIONES ==== #
    # =========================================================================================== #
        

    #Conexion a la BDD
    def connect_db(self):

            self.conn = pymysql.connect(
                host = self.mysql_host,
                user = self.mysql_user,
                password = self.mysql_password,
                database = self.database
            )

            self.cursor = self.conn.cursor()

    def close_conections(self):

        # Confirmar los cambios en la base de datos y cerramos conexiones
        self.conn.commit()
        self.cursor.close()
        self.conn.close()



# =========================================================================================== #
                # ==== SECCION CORRESPONDIENTE AL DATALAKE ==== #
# =========================================================================================== #
        


    #Objetivo: Almacenar los datos de CBA y CBT sin procesar en el datalake. Datos sin procesar
    def load_datalake(self,df):
        
        #Obtenemos los tamanios
        tamanio_df,tamanio_bdd = self.determinar_tamanios(df)

        if tamanio_df > tamanio_bdd: #Si el DF es mayor que lo almacenado, cargar los datos nuevos
            
            #Es necesario el borrado ya que posteriormente las estimaciones tendremos que recalcularlas
            delete_query = 'TRUNCATE cbt_cba'
            self.cursor.execute(delete_query)


            self.cargar_tabla_datalake(df)
            print("==== SE CARGARON DATOS NUEVOS CORRESPONDIENTES A CBT Y CBA DEL DATALAKE ====")


            #Nos vamos a reconectar al DWH de sociodemografico para enviar el correo
            self.set_database("dwh_sociodemografico")
            self.connect_db()
            


            """"
            
            ZONA A CARGAR LAS TABLAS CORRESPONDIENTES A DWH
                        
            """
            
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
        

# =========================================================================================== #
                # ==== SECCION CORRESPONDIENTE AL DATAWAREHOUSE ==== #
# =========================================================================================== #

    """
    Tabla a1: Contiene los datos que corresponden al correo de CBA y CBT.
    Datos:
        - Fecha
        - CBA de GBA
        - CBT de GBA
        - CBA de NEA 
        - CBT de NEA
        - CBA FAMILIAR EN EL NEA
        - CBT FAMILIAR EN EL NEA
        - Var. Mensual, Interanual, Y acumulado de CBA y CBT del NEA.
        - Var. mensual e Interanual de IPC.

    """
    def table_a1(self):

        #Conectamos la BDD para empezar a hacer los calculos,
        self.connect_db()
        df = self.create_date_a1()
        self.load_date_mail(df)



    #Objetivo: calcular los valores necesarios para tabla A1. Los datos de IPC lo tratamos en otra funcion
    def create_date_a1(self):

        #Extraemos datalake
        select_query = "SELECT * FROM cbt_cba"
        df_bdd = pd.read_sql(select_query,self.conn)

        
        df = pd.DataFrame()


        #Asignacion de CBA y CBT de nacion y del NEA. Tambien fecha
        df['fecha'] = df_bdd['fecha']
        df['cba_gba'] = df_bdd['cba_adulto']
        df['cbt_gna'] =  df_bdd['cbt_adulto']
        df['cba_nea'] = df_bdd['cba_nea']
        df['cbt_nea'] = df_bdd['cbt_nea']

        #Asignacion de los valores familiares en el nea. Valor de ponderacion: 3.09
        df['cba_nea_familia'] = df['cba_nea'] * 3.09
        df['cbt_nea_familia'] = df['cbt_nea'] * 3.09


        #==== CALCULOS DE LA CANASTA BASICA ALIMENTARIA

        #=== Creacion de variaciones mensual, interanual PARA CBA
        df['vmensual_cba'] = ((df['cba_nea'] / df['cba_nea'].shift(1)) - 1) * 100  #--> Var. Mensual de cba NEA
        df['vinter_cba'] = ((df['cba_nea'] / df['cba_nea'].shift(12)) - 1) * 100 #--> Var. Interanual de cba NEA


        # === Creacion de las variaciones acumuladas
        #Para el acumulado necesitamos detectar diciembre, vamos a usar la fecha maxima para esto

        df['fecha'] = pd.to_datetime(df['fecha'])#--> Transformamos los datos para maniobrarlos

        #Tomamos los años para recorrerlos
        anios = list(set(df['fecha'].dt.year))

        #Lista de variaciones acumuladas de canasta basica alimentaria
        var_acumuladas_cba = list()

        for anio in anios:

            valores_anio = list(df['cba_nea'][df['fecha'].dt.year == anio].values)

            #Rescatamos valor diciembre del año anterior - Si falla quiere decir que no tenemos ese dato
            try:
                val_diciembre_año_anterior = df['cba_nea'][ (df['fecha'].dt.year == (anio - 1)) & (df['fecha'].dt.month == 12) ].values[0] #--> Obtencion del valor puro de cba NEA
                #Calculamos variaciones acumuladas por cada año valido
                for valor in valores_anio:

                    var_acumulada = ((valor / val_diciembre_año_anterior) - 1) * 100
                    var_acumuladas_cba.append(var_acumulada)

            except: #No se encontro el valor de diciembre, por ende no se calculara estimaciones para ese periodo. Se asignan valores nulos
                for valor_error in valores_anio:
                    var_acumuladas_cba.append(None)

        
        #Asignaciones de variaciones acumuladas de la canasta basica alimentaria      
        df['vacum_cba'] = var_acumuladas_cba

        #==== CALCULOS DE LA CANASTA BASICA TOTAL
        
        #=== Creacion de variaciones mensual, interanual PARA CBT
        df['vmensual_cbt'] = ((df['cbt_nea'] / df['cbt_nea'].shift(1)) - 1) * 100  #--> Var. Mensual de cbt NEA
        df['vinter_cbt'] = ((df['cbt_nea'] / df['cbt_nea'].shift(12)) - 1) * 100 #--> Var. Interanual de cbt NEA

        #Lista de variaciones acumuladas de canasta basica alimentaria
        var_acumuladas_cbt = list()

        for anio in anios:

            valores_anio = list(df['cbt_nea'][df['fecha'].dt.year == anio].values)

            #Rescatamos valor diciembre del año anterior - Si falla quiere decir que no tenemos ese dato
            try:
                val_diciembre_año_anterior = df['cbt_nea'][ (df['fecha'].dt.year == (anio - 1)) & (df['fecha'].dt.month == 12) ].values[0] #--> Obtencion del valor puro de cbt NEA

                #Calculamos variaciones acumuladas por cada año valido
                for valor in valores_anio:

                    var_acumulada = ((valor / val_diciembre_año_anterior) - 1) * 100
                    var_acumuladas_cbt.append(var_acumulada)

            except: #No se encontro el valor de diciembre, por ende no se calculara estimaciones para ese periodo. Se asignan valores nulos
                for valor_error in valores_anio:
                    var_acumuladas_cbt.append(None)

        #Asignaciones de variaciones acumuladas de la canasta basica total
        df['vacum_cbt'] = var_acumuladas_cbt

        

        #==== INCORPORACION DE IPC

        #Para añadir IPC es necesario cerrar las conexiones con la base de datos de sociodemografico y abrirlas con la de economico
        self.close_conections()#--> Cerramos
        self.set_database("datalake_economico") #--> Cambiamos BDD
        self.connect_db() #--> Reconectarnos al datalake economico
        
        #Calculamos las variaciones necesarias del IPC
        self.connecting_with_ipc(df)

        #Cerramos las conexiones nuevamente
        self.close_conections()
        

        return df


    #En esta funcion realizamos los calculos sobre las variaciones de IPC
    def connecting_with_ipc(self,df):

        #Explicacion: la consulta ira a ipc_valores, y traera todos los datos de la region 5 (NEA) y de la categoria 1 (Nivel general del IPC)
        query_consulta = f'SELECT * FROM ipc_valores WHERE ID_Region = 5 and ID_Categoria = 1'

        #Construccion de dataframe a partir de la consulta
        df_bdd = pd.read_sql(query_consulta,self.conn)

        print(df_bdd)        
        # ==== CALCULOS DE VARIACIONES MENSUALES Y INTERANUALES

        #=== Creamos columnas para evitar conflictos de manipulacion del datafarame
        df['vmensual_nea_ipc'] = float('nan')
        df['vinter_nea_ipc'] = float('nan')

        #=== Tomamos la primera fecha del grupo de IPC -- Esto porque CBT y CBA empieza su historico antes que los datos oficiales de IPC
        firt_date =pd.to_datetime (df_bdd['fecha'].values[0])

        #=== Creacion de variaciones mensual, interanual PARA IPC del NEA
        df['vmensual_nea_ipc'].iloc[df['fecha'] >= firt_date] =( df_bdd['valor'] / df_bdd['valor'].shift(1) - 1) * 100  #--> Var. Mensual de IPC
        df['vinter_nea_ipc'].iloc[df['fecha'] >= firt_date] = ((df_bdd['valor'] / df_bdd['valor'].shift(12)) - 1) * 100 #--> Var. Interanual de IPC


    #Objetivo: cargar los datos correspondientes al correo de CBT y CBA. Es llamado en la funcion table_a1()
    def load_date_mail(self,df):
        
        """
        Para la carga es necesario:
        1 - Cambiar por setter el nombre de la bdd
        2 - Conectarnos a la bdd
        3 - Truncar la tabla correspondiente (ya que trabajamos con estimaciones)
        4 - Cargar los datos nuevos
        """

        #Paso 1 - vamos a usar DWH de socio
        self.set_database("dwh_sociodemografico")

        #Paso 2- Conectamos a la bdd
        self.connect_db()

        #Paso 3 - Truncamos la tabla usando la query y el conector. Ejecutamos la consulta
        query_delete_table = "TRUNCATE correo_cbt_cba"
        self.cursor.execute(query_delete_table)


        #4 - Cargamos los datos usando una query y el conector. Ejecutamos las consultas. PARA ESTE PASO ES OBLIGATORIO TRABAJAR CON SQLAlchemy
        engine = create_engine(f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}:{3306}/{self.database}")
        df.to_sql(name="correo_cbt_cba", con=engine, if_exists='replace', index=False)

        print("======")
        print("Los datos correspondiente a los corrreos de CBT y CBA han sido actualizados.")
        print("======")

# =========================================================================================== #        
# =========================================================================================== #


    def carga_db(self):

        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path = os.path.join(ruta_carpeta_files, 'Calculos.xlsx')

        #Conectar la BDD
        self.connect_db()
        

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
        connection_string = f"mysql+mysqlconnector://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}/{self.database}"

        # Crear una conexión a la base de datos utilizando SQLAlchemy
        engine = create_engine(connection_string)

        select_row_count_query = "SELECT COUNT(*) FROM canasta_basica"
        self.cursor.execute(select_row_count_query)
        row_count_before = self.cursor.fetchone()[0]
        
        delete_query ="TRUNCATE `ipecd_economico`.`canasta_basica`"
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
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com', 'samaniego18@gmail.com', 'guillermobenasulin@gmail.com', 'leclerc.mauricio@gmail.com']
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com']
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

        valor_dic_anio_anterior_cba = df_bdd['CBA_nea'][ (df_bdd['fecha'].dt.year == 2023) & (df_bdd['fecha'].dt.month == 12) ].values[0]
        valor_dic_anio_anterior_cbt = df_bdd['CBT_nea'][ (df_bdd['fecha'].dt.year == 2023) & (df_bdd['fecha'].dt.month == 12) ].values[0]

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
        mes_anterior = 12

        #Convertimos la serie a datetime para buscar por año y mes
        df_bdd['Fecha'] = pd.to_datetime(df_bdd['Fecha'])    


        grupo_mes_anterior = df_bdd[ (df_bdd['Fecha'].dt.year == 2023) & (df_bdd['Fecha'].dt.month == mes_anterior)]

        #Total del mes anterior y calculo de la variacion mensual
        total_mes_anterior = grupo_mes_anterior['Valor'].values[0]

        variacion_mensual = ((total_ipc/ total_mes_anterior) - 1) * 100


        #=== CALCULO VARIACION INTERANUAL

        grupo_mes_actual_anio_anterior= df_bdd[(df_bdd['Fecha'].dt.year == 2023 ) & (df_bdd['Fecha'].dt.month == fecha_max.month)]

        total_ipc_anio_anterior =  grupo_mes_actual_anio_anterior['Valor'].values[0]

        variacion_interanual = ((total_ipc / total_ipc_anio_anterior) - 1) * 100



        #=== CALCULO VARIACION ACUMULADA - Variacion desde DIC del año anterior

        grupo_diciembre_anio_anterior = df_bdd[ (df_bdd['Fecha'].dt.year == 2023) & ((df_bdd['Fecha'].dt.month == 12)) ]

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
        cba_mes_anterior = "2023-12"
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

