import mysql
import mysql.connector
import datetime
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd
from armadoXLSDataNacion import LoadXLSDataNacion
from datetime import datetime
import calendar
import os
import xlrd
from sqlalchemy import create_engine


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
        


        print("\n****************************************************")
        print("************* Inicio de la sección IPC *************")
        print("\n****************************************************")
        
        self.conectar_bdd()

        # === Construccion del DF
        df = pd.DataFrame()        
        df['fecha'] = self.lista_fechas
        df['id_region'] = self.lista_region
        df['id_categoria'] = self.lista_categoria
        df['id_division']= self.lista_division
        df['id_subdivision']= self.lista_subdivision
        df['valor'] = self.lista_valores

        

        query_contador = "SELECT COUNT(*) FROM datalake_economico.ipc_valores"
        self.cursor.execute(query_contador)
        row_count_before = self.cursor.fetchone()[0]

        print("CANTIDA  DE DATOS RPEVIA: ",row_count_before)


        query_truncate = "TRUNCATE datalake_economico.ipc_valores"
        self.cursor.execute(query_truncate)


        #Cargamos los datos usando una query y el conector. Ejecutamos las consultas
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")
        df.to_sql(name="ipc_valores", con=engine, if_exists='append', index=False)

        
        # Confirmar los cambios en la base de datos
        self.conn.commit()
        # Cerrar el cursor y la conexión
        self.cursor.close()
        self.conn.close()

        #CARGA DE LOS DATOS DE ID NACION
        LoadXLSDataNacion().loadInDataBase(self.host, self.user, self.password, self.database)


        self.conectar_bdd()



        self.verificar_cantidad(row_count_before)


        # Cerrar el cursor y la conexión
        self.cursor.close()
        self.conn.close()


        


    def enviar_correo(self):
        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        email_receptores =  ['samaniego18@gmail.com','benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com','alejandrobrunel@gmail.com']
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com', 'manumarder@gmail.com']

        
        #Variaciones nacionales
        mensaje_variaciones,fecha = self.variaciones(1)
        mensaje_variaciones_nea,fecha = self.variaciones(5)


        fecha_ultimo_registro = self.obtener_mes_actual(fecha)

        asunto = f'ACTUALIZACION - INDICE DE PRECIOS AL CONSUMIDOR (IPC) - {fecha_ultimo_registro}'

        mensaje = f'''
        
        <html>
        <h2 style="font-size: 24px;"><strong>NUEVOS DATOS DEL INDICE DE PRECIOS AL CONSUMIDOR (IPC) DE {fecha_ultimo_registro.upper()}.</strong></h2>


        <body>

        <hr>

        '''
        
        #Aderimos al mensje las variaciones nacionales
        variaciones_nacionales = f'''

        <h3> Variaciones a nivel Nacional - Argentina </h3>

        ''' + mensaje_variaciones + "<hr>"

        variaciones_nea = f'''

                <h3> Variaciones Nordeste(NEA) - Argentina </h3>


        ''' + mensaje_variaciones_nea + "<hr>"


        #Creacion de tabla con variaciones
        datos_tabla = self.variaciones_nea()

        tabla = f''' 
        
        <table style="border-collapse: collapse; width: 100%; margin-bottom: 10px;">

        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;">  VAR. MENSUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VAR. INTERANUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VAR. ACUMULADA </th>
        {datos_tabla}

        </table> 
        
        <hr>

            <p> Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes<br>
            Dirección: Tucumán 1164 - Corrientes Capital<br>
            Contacto Coordinación General: 3794 284993</p>


        </body>
        </html>
        '''                                                                                                                                                                                                                                      
        cadena_final = mensaje + variaciones_nacionales + variaciones_nea + tabla
        
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = asunto
        em.set_content(cadena_final, subtype = 'html')      
        contexto = ssl.create_default_context()
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())


    def verificar_cantidad(self,row_count_before):
       

        #Verificar cantidad de filas anteriores 
        select_row_count_query = "SELECT COUNT(*) FROM ipc_valores"
        #Obtener cantidad de filas
        self.cursor.execute(select_row_count_query)
        row_count_after = self.cursor.fetchone()[0]

        print(f"\n - LA CANTIDAD DE FILAS EN LA BDD ES: {row_count_after}")
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

        nombre_tabla = 'ipc_valores'

        query_consulta = f'SELECT * FROM {nombre_tabla} WHERE id_region = {region} and id_categoria = 1'

        #query_prueba = 'SELECT * FROM ipc_valores WHERE id_region = 1 and id_categoria = 1'
        df_bdd = pd.read_sql(query_consulta,self.conn)

        print("DF EXTRAIDO DE LA BDD")
        print(df_bdd)

        #==== IPC registrado en el AÑO Y MES actual ==== #

        #Obtener ultima fecha
        fecha_max = df_bdd['fecha'].max()
        
        #Buscamos los registros de la ultima fecha - Sumamos todos los campos
        grupo_ultima_fecha = df_bdd[(df_bdd['fecha'] == fecha_max)]

        total_ipc = grupo_ultima_fecha['valor'].values[0]

         #=== CALCULO DE LA VARIACION MENSUAL
        
        #Año actual
        anio_actual = fecha_max.year

        #Buscamos el mes anterior
        mes_anterior = int(fecha_max.month) - 1

        if mes_anterior == 0:
            mes_anterior = 12
            anio_actual = fecha_max.year - 1


        #Convertimos la serie a datetime para buscar por año y mes
        df_bdd['fecha'] = pd.to_datetime(df_bdd['fecha'])    
        grupo_mes_anterior = df_bdd[ (df_bdd['fecha'].dt.year == anio_actual) & (df_bdd['fecha'].dt.month == mes_anterior)]

        #Total del mes anterior y calculo de la variacion mensual
        total_mes_anterior = grupo_mes_anterior['valor'].values[0]

        variacion_mensual = ((total_ipc/ total_mes_anterior) - 1) * 100


        #=== CALCULO VARIACION INTERANUAL

        grupo_mes_actual_año_anterior= df_bdd[(df_bdd['fecha'].dt.year == fecha_max.year-1 ) & (df_bdd['fecha'].dt.month == fecha_max.month)]

        total_ipc_año_anterior =  grupo_mes_actual_año_anterior['valor'].values[0]

        variacion_interanual = ((total_ipc / total_ipc_año_anterior) - 1) * 100



        #=== CALCULO VARIACION ACUMULADA - Variacion desde DIC del año anterior

        grupo_diciembre_año_anterior = df_bdd[ (df_bdd['fecha'].dt.year == fecha_max.year-1 ) & ((df_bdd['fecha'].dt.month == 12)) ]

        total_diciembre = grupo_diciembre_año_anterior['valor'].values[0]

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

        cadena_variaciones =f'''
        <p>VARIACION MENSUAL: <span style="font-size: 17px;"><b>{numero_truncado_mensual}%</b></span></p>
        <p>VARIACION INTERANUAL: <span style="font-size: 17px;"><b>{numero_truncado_interanual}%</b></span></p>
        <p>VARIACION ACUMULADA: <span style="font-size: 17px;"><b>{numero_truncado_acumulado}%</b></span></p>
        '''

        return cadena_variaciones,fecha_max

    #Objetivo: calcular las variaciones de las subdiviones del NEA
    def variaciones_nea(self):

        #Lista de variaciones
        list_var_mensual = []
        list_var_interanual = []
        list_var_acumulada = []
        lista_indices = []

        #Cadena que contendra todos los datos en formato HTML
        cadena_de_datos =''

        #Buscamos tabla con los datos del IPC
        nombre_tabla = 'ipc_valores'
        query_consulta = f'SELECT * FROM {nombre_tabla} WHERE id_region = 5'
        df_bdd = pd.read_sql(query_consulta,self.conn)  

        #Buscamos la tabla de las subdivisiones para imprimir los mensajes
        nombre_subdivision = 'ipc_categoria'
        query_consulta = f'SELECT * FROM {nombre_subdivision}'
        df_subdivisiones = pd.read_sql(query_consulta,self.conn)
        
        #Obtener ultima fecha
        fecha_max = df_bdd['fecha'].max()

        #mes y año actuales
        mes_actual = fecha_max.month
        anio_actual = fecha_max.year


        #=== OPERACIONES NECESARIAS PARA EL CALCULO DE LA VARIACION MENSUAL

        #Buscamos los registros de la ultima fecha 
        grupo_ultima_fecha = df_bdd[(df_bdd['fecha'] == fecha_max)]

        #Buscamos el mes anterior
        mes_anterior = int(fecha_max.month) - 1

        if mes_anterior == 0:
            mes_anterior = 12
            anio_actual = fecha_max.year - 1



        #Convertimos la serie a datetime para buscar por año y mes
        df_bdd['fecha'] = pd.to_datetime(df_bdd['fecha'])    
        grupo_mes_anterior = df_bdd[ (df_bdd['fecha'].dt.year == anio_actual) & (df_bdd['fecha'].dt.month == mes_anterior)]


        #=== OPERACIONES NECESARIAS PARA EL CALCULO DE LA VARIACION INTERANUAL
        grupo_año_anterior = df_bdd[ (df_bdd['fecha'].dt.year == fecha_max.year - 1) & (df_bdd['fecha'].dt.month == fecha_max.month)]


        #=== OPERACIONES NECESARIAS PARA EL CALCULO DE LA VARIACION ACUMULADA
        grupo_dic_año_anterior = df_bdd[ (df_bdd['fecha'].dt.year == fecha_max.year - 1) & (df_bdd['fecha'].dt.month == 12)]

        #Vamos a detectar los valores de cada subdivision y calcular su variacion mensual
        #Luego lo agregaremos a una tabla de STR o HTML para trabajar el conjunto en su totalidad
        for indice in grupo_ultima_fecha['id_categoria'].unique():
            
            #=== CALCULO DE LA VARIACION MENSUAL

            #Busqueda del valor del mes anterior
            fila_mes_anterior = grupo_mes_anterior[grupo_mes_anterior['id_categoria'] == indice]
            valor_mes_anterior = fila_mes_anterior['valor'].values[0]

            #valor del valor del mes actual
            fila_mes_actual = grupo_ultima_fecha[grupo_ultima_fecha['id_categoria'] == indice]
            valor_mes_actual = fila_mes_actual['valor'].values[0]

            #Busqueda del nombre de la subdivision 
            subdivision = df_subdivisiones[df_subdivisiones['id_categoria'] == indice]
            nombre_indice = subdivision['nombre'].values[0]

            #Calculo de la variacion mensual
            var_mensual = ((valor_mes_actual / valor_mes_anterior) - 1 ) * 100            

            #=== CALCULO DE LA VARIACION INTERANUAL

            #Busqueda del valor del año anterior 
            fila_año_anterior = grupo_año_anterior[grupo_año_anterior['id_categoria'] == indice]
            valor_año_anterior = fila_año_anterior['valor'].values[0]

            
            var_interanual = ((valor_mes_actual / valor_año_anterior) - 1) * 100


            #=== CALCULO DE LA VARIACION ACUMULADA

            #Busqueda del valor del DICIEMBRRE del anterior 
            fila_dic_año_anterior = grupo_dic_año_anterior[grupo_dic_año_anterior['id_categoria'] == indice]
            valor_dic_año_anterior = fila_dic_año_anterior['valor'].values[0]

            
            var_acumulada = ((valor_mes_actual / valor_dic_año_anterior) - 1) * 100


            list_var_mensual.append(var_mensual)
            list_var_interanual.append(var_interanual) 
            list_var_acumulada.append(var_acumulada)
            lista_indices.append(nombre_indice)



        #Creacion de DATAFRAME
        data = {
             
             'nombre_indices':lista_indices,
             'var_mensual': list_var_mensual,
             'var_interanual':list_var_interanual,
             'var_acumulada':list_var_acumulada
        }

        df = pd.DataFrame(data)

        #Ordenacion por var mensual | intearanual| acumulado
        df_mensual = df.sort_values(by='var_mensual',ascending=[False])
        df_interanual = df.sort_values(by='var_interanual',ascending=[False])
        df_acumulado = df.sort_values(by='var_acumulada',ascending=[False])

        #Armado de cadena para correo
        for nombre_mensual,var_mensual,nombre_interanual,var_interanual, nombre_acumulado,var_acumulada in zip(df_mensual['nombre_indices'],df_mensual['var_mensual'],df_interanual['nombre_indices'],df_interanual['var_interanual'],df_acumulado['nombre_indices'],df_acumulado['var_acumulada']):
        
            fila_de_nea = f'''
                <tr>

                 <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {nombre_mensual}</td>
                 <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {var_mensual:.2f}%</td>

                 <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {nombre_interanual}</td>
                 <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {var_interanual:.2f}%</td>

                 <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {nombre_acumulado}</td>
                 <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {var_acumulada:.2f}%</td>
                </tr>
                '''

            cadena_de_datos = cadena_de_datos + fila_de_nea
    
        
        return cadena_de_datos
        
        
    def obtener_mes_actual(self,fecha_ultimo_registro):
         

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


    def var_mensual_prueba(self):


        #Construccion de la direccion
        directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
        file_path_desagregado = os.path.join(ruta_carpeta_files, 'IPC_Desagregado.xls')
         
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




