import mysql
import mysql.connector
import datetime
from email.message import EmailMessage
import ssl
import smtplib
import pandas as pd

class conexionBaseDatos:

    #Inicializacion de variables en la clase
    def __init__(self, host, user, password, database, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas):

        self.host = host
        self.user = user
        self.password = password 
        self.database = database
        self.lista_provincias = lista_provincias
        self.lista_valores_estacionalidad = lista_valores_estacionalidad
        self.lista_valores_sin_estacionalidad = lista_valores_sin_estacionalidad
        self.lista_registro = lista_registro
        self.lista_fechas = lista_fechas
        self.cursor = None
        self.conn = None

    def conectar_bdd(self,host,user,password,database):

            self.conn = mysql.connector.connect(
                host=host, user=user, password=password, database=database
            )
            self.cursor = self.conn.cursor()

        

    def cargaBaseDatos(self):

        try:
            #Conectar a la BDD 
            self.conectar_bdd(self.host,self.user,self.password,self.database)
            
            #Se le asigna la lista correspondiente a la columna del data frame y se arma el "Excel"
            df = pd.DataFrame() 
            df['fecha'] = self.lista_fechas
            df['id_prov'] = self.lista_provincias
            df['tipo_registro'] = self.lista_registro
            df['valores_estacionales'] = self.lista_valores_estacionalidad
            df['valores_no_estacionales'] = self.lista_valores_sin_estacionalidad
            
            # Verificar cuantas filas tiene la tabla de mysql
            select_query = "SELECT COUNT(*) FROM sipa_registro WHERE Fecha = %s"
            
            # Sentencia SQL para insertar los datos en la tabla sipa_registro
            insert_query = "INSERT INTO sipa_registro (Fecha, ID_Provincia, ID_Tipo_Registro, Cantidad_con_Estacionalidad, Cantidad_sin_Estacionalidad) VALUES (%s, %s, %s, %s, %s)"

            # Sentencia SQL para actualizar los datos en la tabla
            update_query = "UPDATE sipa_registro SET Cantidad_con_Estacionalidad = %s, Cantidad_sin_Estacionalidad = %s WHERE Fecha = %s AND ID_Provincia = %s AND ID_Tipo_Registro = %s"

            #Verificar cantidad de filas anteriores 
            select_row_count_query = "SELECT COUNT(*) FROM sipa_registro"
            self.cursor.execute(select_row_count_query)
            row_count_before = self.cursor.fetchone()[0]
            
            #Borrado de tabla para actualizacion
            delete_query ="TRUNCATE `prueba1`.`sipa_registro`"
            self.cursor.execute(delete_query)
            
            for fecha, id_prov, tipo_registro, valores_estacionales, valores_no_estacionales in zip(self.lista_fechas, self.lista_provincias, self.lista_registro, self.lista_valores_estacionalidad, self.lista_valores_sin_estacionalidad):
                # Convertir la fecha en formato datetime si es necesario
                if isinstance(fecha, str):
                    fecha = datetime.datetime.strptime(fecha, '%Y-%m-%d').date()

                self.cursor.execute(insert_query, (fecha, id_prov, tipo_registro, valores_estacionales, valores_no_estacionales))
                        
            #Obtener cantidad de filas
            self.cursor.execute(select_row_count_query)
            row_count_after = self.cursor.fetchone()[0]

            #Comparar la cantidad de antes y despues
            if row_count_after > row_count_before:
                print("Se agregaron nuevos datos")
                self.enviar_correo()   
            else:
                print("Se realizo una verificacion de la base de datos")
                
            # Confirmar los cambios en la base de datos
            self.conn.commit()
            # Cerrar el cursor y la conexión
            self.cursor.close()
            self.conn.close()

        except Exception as e:
            
            print(e)   

    def enviar_correo(self):


        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com']


        #PORCENTAJES DE EMPLEOS REGISTRADOS
        porcentaje_privado, porcentaje_publico, porcentaje_total_casas_particulares,porcentaje_total_idp_autonomo,porcentaje_total_idp_monotributo,porcentaje_total_idp_monotributo_social,cadena_ultima_fecha = self.obtener_porcentaje_clases()


        #EMPLEO REGISTRADO A NIVEL PAIS - GENERAL
        total_nivel_pais, variacion_mensual, variacion_interanual,diferencia_mensual,diferencia_interanual = self.empleo_registrado_pais()

        #EMPLEO PRIVADO REGISTRADO A NIVEL PAIS 
        total_nivel_pais_privado, variacion_mensual_privado, variacion_interanual_privado, diferencia_mensual_privado,diferencia_interanual_privado,fecha_del_maximo,maximo = self.empleo_privado_registrado()

        #EMPLEO PRIVADO REGISTRADO EN EL NEA
        total_empleo_nea,variacion_mensual_nea,variacion_interanual_nea, diferencia_interanual_nea,diferencia_mensual_nea = self.empleos_nea()

        #Empleo PRIVADO REGISTRADO EN CORRIENTES
        total_empleo_corr,variacion_mensual_corr,variacion_interanual_corr, diferencia_interanual_corr,diferencia_mensual_corr,promedio_empleo_corr = self.empleo_corrientes()

        asunto = f'Modificación en la base de datos - SISTEMA INTEGRADO PREVISIONAL ARGENTINO(SIPA) - Fecha {cadena_ultima_fecha}'
        
        mensaje_uno = f'''\
        <html>
        <body>
        <h2>Se ha producido una modificación en la base de datos. La tabla de SIPA contiene nuevos datos.</h2>

        <hr>
        <h3> Empleos Registrados ({cadena_ultima_fecha}) </h3>
        <p>1 - Empleos privados registrados: <span style="font-size: 17px;"><b>{porcentaje_privado:.2f}%</b></span></p>
        <p>2 - Empleos publicos registrados: <span style="font-size: 17px;"><b>{porcentaje_publico:.2f}%</b></span></p>
        <p>3 - Monotributistas Independientes: <span style="font-size: 17px;"><b>{porcentaje_total_idp_monotributo:.2f}%</b></span></p>
        <p>4 - Monotributistas Sociales: <span style="font-size: 17px;"><b>{porcentaje_total_idp_monotributo_social:.2f}%</b></span></p>
        <p>5 - Empleo en casas particulares registrado: <span style="font-size: 17px;"><b>{porcentaje_total_casas_particulares:.2f}%</b></span></p>
        <p>6 - Trabajadores independientes autonomos: <span style="font-size: 17px;"><b>{porcentaje_total_idp_autonomo:.2f}%</b></span></p>
        <hr>
        <h3> Empleo Registrado a nivel pais: </h3>
        <p>Total: <span style="font-size: 17px;"><b>{total_nivel_pais}</b></span></p>
        <p>Variacion mensual: <span style="font-size: 17px;"><b>{variacion_mensual:.2f}%</b></span> ({diferencia_mensual}) puestos  </p>
        <p>Variacion interanual: <span style="font-size: 17px;"><b>{variacion_interanual:.2f}%</b></span>  ({diferencia_interanual}) puestos  </p>
        <hr>
        <h3> Empleo PRIVADO registrado a nivel pais: </h3>
        <p>Total: <span style="font-size: 17px;"><b>{total_nivel_pais_privado}</b></span></p>
        <p>Variacion mensual: <span style="font-size: 17px;"><b>{variacion_mensual_privado:.2f}%</b></span>  ({diferencia_mensual_privado}) puestos  </p>
        <p>Variacion interanual: <span style="font-size: 17px;"><b>{variacion_interanual_privado:.2f}%</b></span> ({diferencia_interanual_privado}) puestos </p>
        <p> Maximo HISTORICO - FECHA {fecha_del_maximo} - Total de empleo privado en la fecha: {maximo}
        <hr>
        <h3>Empleo PRIVADO registrado en el NEA (Nordeste Argentino) </h3>
        <p>Total: <span style="font-size: 17px;"><b>{total_empleo_nea}</b></span></p>
        <p>Variacion mensual: <span style="font-size: 17px;"><b>{variacion_mensual_nea:.2f}%</b></span>  ({diferencia_mensual_nea}) puestos  </p>
        <p>Variacion interanual: <span style="font-size: 17px;"><b>{variacion_interanual_nea:.2f}%</b></span> ({diferencia_interanual_nea}) puestos </p>
        <hr>
        <h3>Empleo PRIVADO registrado en CORRIENTES</h3>
        <p>Total: <span style="font-size: 17px;"><b>{total_empleo_corr}</b></span></p>
        <p>Variacion mensual: <span style="font-size: 17px;"><b>{variacion_mensual_corr:.2f}%</b></span>  ({diferencia_mensual_corr}) puestos  </p>
        <p>Variacion interanual: <span style="font-size: 17px;"><b>{variacion_interanual_corr:.2f}%</b></span> ({diferencia_interanual_corr}) puestos </p>
        <p>Promedio de empleo por mes en el año actual: <span style="font-size: 17px;"><b>{promedio_empleo_corr}</b></span></p>
        <hr>
        '''

        mensaje_dos = ''''''
        #Creacion de datos del NEA - mensaje 2
        for cod_provincia in (54,22,34):

            total_empleo_otra,variacion_mensual_otra,variacion_interanual_otra, diferencia_interanual_otra,diferencia_mensual_otra,nombre_prov_otra = self.empleo_otras_nea(cod_provincia)

            cadena_aux =f''' 
                <h3>Empleo PRIVADO registrado en {nombre_prov_otra}</h3>
                <p>Total: <span style="font-size: 17px;"><b>{total_empleo_otra}</b></span></p>
                <p>Variacion mensual: <span style="font-size: 17px;"><b>{variacion_mensual_otra:.2f}%</b></span>  ({diferencia_mensual_otra}) puestos  </p>
                <p>Variacion interanual: <span style="font-size: 17px;"><b>{variacion_interanual_otra:.2f}%</b></span> ({diferencia_interanual_otra}) puestos </p>
                <hr>
                '''
            
            mensaje_dos = mensaje_dos + cadena_aux

        mensaje_tres = '''
                        <p> Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes<br>
                                 Dirección: Tucumán 1164 - Corrientes Capital<br>
                             Contacto Coordinación General: 3794 284993</p>
                        </body>
                        </html>
                        '''

        
        mensaje = mensaje_uno + mensaje_dos + mensaje_tres
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = asunto
        em.set_content(mensaje, subtype = 'html')
        
        contexto= ssl.create_default_context()
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())


    """
    En estas funciones nos encargamos de obtener los datos:

    * El Porcentaje(%) de cada grupo
    * Empleo total registrado a nivel pais - Variancion mensual e Interanual del mes actual
    * Empleo privado registraddo -  Variancion mensual e Interanual del mes actual
    * Empleo registrado PROMEDIO NEA - Variancion mensual e Interanual del mes actual
    * Empleo registrado en Corrientes - Variancion mensual e Interanual del mes actual

    """
    def obtener_porcentaje_clases(self):

        # ====  Carga de datos desde la BDD - transformacion a DATAFRAME
        nombre_tabla = 'sipa_registro'
        query_carga = f'SELECT * FROM {nombre_tabla}'
        df_bdd = pd.read_sql(query_carga,self.conn)


        # === Calculando el porcentaje(%) de cada grupo
        grupos = df_bdd.groupby('ID_Tipo_Registro')

        print("\n\n")

        #Variables de empleo
        total_privado = None
        total_publico = None
        total_casas_particulares = None
        total_idp_autonomo = None
        total_idp_monotributo = None
        total_idp_monotributo_social = None

        lista_totales = []
        

        #Para los totales no tenemos en cuenta GRUPO 1 (NACION) y el GRUPO 8(Totales)
        for categoria,grupo in grupos:  

            if categoria == 1 or categoria == 8:
                pass
            else:
                #Posicionamiento en la lista
                indice = categoria-2

                #Obtencion de fecha MAXIMA
                fecha_max = grupo['Fecha'].max()
                #Obtener la fila 

                fila = grupo[grupo['Fecha'] == fecha_max]

                lista_totales.append(fila['Cantidad_con_Estacionalidad'].values[0])

                print(f"Valor {lista_totales[indice]} - Categoria {categoria} ")
     
        

        #Asignacon de valores

        #Variables de empleo
        total_privado = lista_totales[0]
        total_publico = lista_totales[1]
        total_casas_particulares = lista_totales[2]
        total_idp_autonomo = lista_totales[3]
        total_idp_monotributo = lista_totales[4]
        total_idp_monotributo_social = lista_totales[5]

        #=== CALCULO DE PORCENTAJES

        total = sum(lista_totales)

        porcentaje_privado = ((total_privado * 100) / total)
        porcentaje_publico = ((total_publico * 100) / total)
        porcentaje_total_casas_particulares = ((total_casas_particulares * 100) / total)
        porcentaje_total_idp_autonomo = ((total_idp_autonomo * 100) / total)
        porcentaje_total_idp_monotributo = ((total_idp_monotributo  * 100) / total)
        porcentaje_total_idp_monotributo_social = ((total_idp_monotributo_social  * 100) / total)


        #Ultima fecha registrada
        ultima_fecha = df_bdd['Fecha'].max()
        cadena_ultima_fecha = str(ultima_fecha.year) + "-"+ str(ultima_fecha.month)
        
        return porcentaje_privado, porcentaje_publico, porcentaje_total_casas_particulares,porcentaje_total_idp_autonomo,porcentaje_total_idp_monotributo,porcentaje_total_idp_monotributo_social,cadena_ultima_fecha



    def empleo_registrado_pais(self):

        # ====  Carga de datos desde la BDD - transformacion a DATAFRAME
        nombre_tabla = 'sipa_registro'
        query_carga = f'SELECT * FROM {nombre_tabla}'
        df_bdd = pd.read_sql(query_carga,self.conn)

        df_bdd = df_bdd[(df_bdd['ID_Tipo_Registro'] != 1) & (df_bdd['ID_Tipo_Registro'] != 8)]

        #==== Registrado a nivel pais del AÑO/MES actual ==== #

        #Obtener ultima fecha
        fecha_max = df_bdd['Fecha'].max()

        #Buscamos los registros de la ultima fecha - Ademas ignoramos los registros de TIPO 1(Empleo GENERAL) y de tipo 8 (Total)
        grupo_ultima_fecha = df_bdd[(df_bdd['Fecha'] == fecha_max)]

        #total registrado a nivel pais - Se multiplica por MIL porque el valor esta expresado en miles
        total_nivel_pais = int(sum(grupo_ultima_fecha['Cantidad_con_Estacionalidad']) * 1000)

        #=== CALCULO DE LA VARIACION MENSUAL

        #Buscamos el mes anterior
        mes_anterior = int(fecha_max.month) - 1

        #Convertimos la serie a datetime para buscar por año y mes
        df_bdd['Fecha'] = pd.to_datetime(df_bdd['Fecha'])    
        grupo_mes_anterior = df_bdd[ (df_bdd['Fecha'].dt.year == fecha_max.year) & (df_bdd['Fecha'].dt.month == mes_anterior)]

        #Total del mes anterior
        total_mes_anterior = int(sum(grupo_mes_anterior['Cantidad_con_Estacionalidad']) * 1000)

        variacion_mensual = ((total_nivel_pais/ total_mes_anterior) - 1) * 100

        #=== Calculo de la VARIACION INTERANUAL
        grupo_mes_actual_año_anterior= df_bdd[(df_bdd['Fecha'].dt.year == fecha_max.year-1 ) & (df_bdd['Fecha'].dt.month == fecha_max.month)]

        total_nea_año_anterior = sum(grupo_mes_actual_año_anterior['Cantidad_con_Estacionalidad']) * 1000

        variacion_interanual = ((total_nivel_pais / total_nea_año_anterior) - 1) * 100

        diferencia_mensual = int(total_nivel_pais - total_mes_anterior)
        diferencia_interanual = int(total_nivel_pais - total_nea_año_anterior)




        return total_nivel_pais, variacion_mensual, variacion_interanual, diferencia_mensual,diferencia_interanual

    #Esta funcion es muy parecido a la anterior - En este caso apuntamos solo a los empleos privados
    def empleo_privado_registrado(self):
       
        # ====  Carga de datos desde la BDD - transformacion a DATAFRAME
        nombre_tabla = 'sipa_registro'
        query_carga = f'SELECT * FROM {nombre_tabla}'
        df_bdd = pd.read_sql(query_carga,self.conn)

        df_bdd = df_bdd[(df_bdd['ID_Tipo_Registro'] == 2)]

        #==== Registrado a nivel pais del AÑO/MES actual ==== #

        #Obtener ultima fecha
        fecha_max = df_bdd['Fecha'].max()

        #Buscamos los registros de la ultima fecha - Ademas ignoramos los registros de TIPO 1(Empleo GENERAL) y de tipo 8 (Total)
        grupo_ultima_fecha = df_bdd[(df_bdd['Fecha'] == fecha_max)]

        #total registrado a nivel pais - Se multiplica por MIL porque el valor esta expresado en miles
        total_nivel_pais = int(sum(grupo_ultima_fecha['Cantidad_con_Estacionalidad']) * 1000)

        #=== CALCULO DE LA VARIACION MENSUAL

        #Buscamos el mes anterior
        mes_anterior = int(fecha_max.month) - 1

        #Convertimos la serie a datetime para buscar por año y mes
        df_bdd['Fecha'] = pd.to_datetime(df_bdd['Fecha'])    
        grupo_mes_anterior = df_bdd[ (df_bdd['Fecha'].dt.year == fecha_max.year) & (df_bdd['Fecha'].dt.month == mes_anterior)]

        #Total del mes anterior
        total_mes_anterior = int(sum(grupo_mes_anterior['Cantidad_con_Estacionalidad']) * 1000)

        variacion_mensual = ((total_nivel_pais/ total_mes_anterior) - 1) * 100

        #=== Calculo de la VARIACION INTERANUAL
        grupo_mes_actual_año_anterior= df_bdd[(df_bdd['Fecha'].dt.year == fecha_max.year-1 ) & (df_bdd['Fecha'].dt.month == fecha_max.month)]

        total_nea_año_anterior = sum(grupo_mes_actual_año_anterior['Cantidad_con_Estacionalidad']) * 1000

        variacion_interanual = ((total_nivel_pais / total_nea_año_anterior) - 1) * 100


        diferencia_mensual = int(total_nivel_pais - total_mes_anterior)
        diferencia_interanual = int(total_nivel_pais - total_nea_año_anterior)


         #=== Obtencion del registro de valor maximo registrado
        
        #indice del maximo
        indice_maximo = df_bdd['Cantidad_con_Estacionalidad'].idxmax()
        fila=df_bdd.loc[indice_maximo] 

        fecha_del_maximo = fila['Fecha']
        maximo = int(fila['Cantidad_con_Estacionalidad'] * 1000)

        fecha_del_maximo = str(fecha_del_maximo.year) + "-" + str(fecha_del_maximo.month)


        return total_nivel_pais, variacion_mensual, variacion_interanual, diferencia_mensual,diferencia_interanual, fecha_del_maximo,maximo
     
    #Calcular los valores correspondientes al NEA - Algunas variables se multiplican por 1000 porque asi estan expresadas en los EXCELS
    def empleos_nea(self): 

        #Hacemos una consulta a la BDD, traemos datos correspondientes a: MISIONES(54) + CHACO(22) + CORRIENTES(18) + FORMOSA(34)
        #No hacemos referencia al tipo de registro porque los datos ya estan guardados como PRIVADO
        nombre_tabla = 'sipa_registro'
        query_consulta = f'SELECT * FROM {nombre_tabla} WHERE ID_Provincia IN (54,22,18,34)'
        df_bdd = pd.read_sql(query_consulta,self.conn)

        #Obtener ultima fecha
        fecha_max = df_bdd['Fecha'].max()
        
        #Buscamos los registros de la ultima fecha 
        grupo_ultima_fecha = df_bdd[(df_bdd['Fecha'] == fecha_max)]
        
        total_empleo = int(sum(grupo_ultima_fecha['Cantidad_con_Estacionalidad'])) * 1000

         #=== CALCULO DE LA VARIACION MENSUAL

        #Buscamos el mes anterior
        mes_anterior = int(fecha_max.month) - 1

        #Convertimos la serie a datetime para buscar por año y mes
        df_bdd['Fecha'] = pd.to_datetime(df_bdd['Fecha'])    
        grupo_mes_anterior = df_bdd[ (df_bdd['Fecha'].dt.year == fecha_max.year) & (df_bdd['Fecha'].dt.month == mes_anterior)]

        #Total del mes anterior
        total_mes_anterior = int(sum(grupo_mes_anterior['Cantidad_con_Estacionalidad']) * 1000)

        variacion_mensual = ((total_empleo/ total_mes_anterior) - 1) * 100 

        #=== Calculo de la VARIACION INTERANUAL
        grupo_mes_actual_año_anterior= df_bdd[(df_bdd['Fecha'].dt.year == fecha_max.year-1 ) & (df_bdd['Fecha'].dt.month == fecha_max.month)]
        total_nea_año_anterior = sum(grupo_mes_actual_año_anterior['Cantidad_con_Estacionalidad']) * 1000
        variacion_interanual = ((total_empleo / total_nea_año_anterior) - 1) * 100


        #Calculos de diferencias por MES y AÑO
        diferencia_mensual = int(total_empleo - total_mes_anterior)
        diferencia_interanual = int(total_empleo - total_nea_año_anterior)

        return total_empleo,variacion_mensual,variacion_interanual, diferencia_interanual,diferencia_mensual

    #Datos de CORRIENTES
    def empleo_corrientes(self):
        
        #Hacemos una consulta a la BDD, traemos datos correspondientes a: MISIONES(54) + CHACO(22) + CORRIENTES(18) + FORMOSA(34)
        #No hacemos referencia al tipo de registro porque los datos ya estan guardados como PRIVADO
        nombre_tabla = 'sipa_registro'
        query_consulta = f'SELECT * FROM {nombre_tabla} WHERE ID_Provincia = 18'
        df_bdd = pd.read_sql(query_consulta,self.conn)

        #Obtener ultima fecha
        fecha_max = df_bdd['Fecha'].max()

        #Buscamos los registros de la ultima fecha 
        grupo_ultima_fecha = df_bdd[(df_bdd['Fecha'] == fecha_max)]
        
        total_empleo = int(sum(grupo_ultima_fecha['Cantidad_con_Estacionalidad'])) * 1000

         #=== CALCULO DE LA VARIACION MENSUAL

        #Buscamos el mes anterior
        mes_anterior = int(fecha_max.month) - 1

        #Convertimos la serie a datetime para buscar por año y mes
        df_bdd['Fecha'] = pd.to_datetime(df_bdd['Fecha'])    
        grupo_mes_anterior = df_bdd[ (df_bdd['Fecha'].dt.year == fecha_max.year) & (df_bdd['Fecha'].dt.month == mes_anterior)]

        #Total del mes anterior
        total_mes_anterior = int(sum(grupo_mes_anterior['Cantidad_con_Estacionalidad']) * 1000)

        variacion_mensual = ((total_empleo/ total_mes_anterior) - 1) * 100 

        #=== Calculo de la VARIACION INTERANUAL
        grupo_mes_actual_año_anterior= df_bdd[(df_bdd['Fecha'].dt.year == fecha_max.year-1 ) & (df_bdd['Fecha'].dt.month == fecha_max.month)]
        total_nea_año_anterior = sum(grupo_mes_actual_año_anterior['Cantidad_con_Estacionalidad']) * 1000
        variacion_interanual = ((total_empleo / total_nea_año_anterior) - 1) * 100


        #Calculos de diferencias por MES y AÑO
        diferencia_mensual = int(total_empleo - total_mes_anterior)
        diferencia_interanual = int(total_empleo - total_nea_año_anterior)


        #=== Calculo del PROMEDIO de empleo DEL AÑO ACTUAL
        grupo_año_actual = df_bdd[(df_bdd['Fecha'].dt.year == fecha_max.year )]
        promedio_empleo = int(sum(grupo_año_actual['Cantidad_con_Estacionalidad'])) / len(grupo_año_actual['Cantidad_con_Estacionalidad'])


        return total_empleo,variacion_mensual,variacion_interanual, diferencia_interanual,diferencia_mensual,promedio_empleo

    #Datos de las demas pronvincias del nea
    def empleo_otras_nea(self,cod_prov):

        #No hacemos referencia al tipo de registro porque los datos ya estan guardados como PRIVADO
        nombre_tabla = 'sipa_registro'
        nombre_tabla_provincias = 'dp_provincias'

        query_consulta = f'SELECT * FROM {nombre_tabla} WHERE ID_Provincia = {cod_prov}'
        query_nombre_prov = f'SELECT nombre_provincia_indec FROM {nombre_tabla_provincias} WHERE id_provincia_indec = {cod_prov}'

        #Construir dataframe | Obtener nombre de la provincia
        df_bdd = pd.read_sql(query_consulta,self.conn)
        print(df_bdd)
        self.cursor.execute(query_nombre_prov)
        resultado = self.cursor.fetchone()  # Esto obtendrá la primera fila de resultado
        nombre_prov = resultado[0]

        #Obtener ultima fecha
        fecha_max = df_bdd['Fecha'].max()

        #Buscamos los registros de la ultima fecha 
        grupo_ultima_fecha = df_bdd[(df_bdd['Fecha'] == fecha_max)]
        
        total_empleo = int(sum(grupo_ultima_fecha['Cantidad_con_Estacionalidad'])) * 1000

         #=== CALCULO DE LA VARIACION MENSUAL

        #Buscamos el mes anterior
        mes_anterior = int(fecha_max.month) - 1

        #Convertimos la serie a datetime para buscar por año y mes
        df_bdd['Fecha'] = pd.to_datetime(df_bdd['Fecha'])    
        grupo_mes_anterior = df_bdd[ (df_bdd['Fecha'].dt.year == fecha_max.year) & (df_bdd['Fecha'].dt.month == mes_anterior)]

        #Total del mes anterior
        total_mes_anterior = int(sum(grupo_mes_anterior['Cantidad_con_Estacionalidad']) * 1000)

        variacion_mensual = ((total_empleo/ total_mes_anterior) - 1) * 100 

        #=== Calculo de la VARIACION INTERANUAL
        grupo_mes_actual_año_anterior= df_bdd[(df_bdd['Fecha'].dt.year == fecha_max.year-1 ) & (df_bdd['Fecha'].dt.month == fecha_max.month)]
        total_nea_año_anterior = sum(grupo_mes_actual_año_anterior['Cantidad_con_Estacionalidad']) * 1000
        variacion_interanual = ((total_empleo / total_nea_año_anterior) - 1) * 100


        #Calculos de diferencias por MES y AÑO
        diferencia_mensual = int(total_empleo - total_mes_anterior)
        diferencia_interanual = int(total_empleo - total_nea_año_anterior)


        return total_empleo,variacion_mensual,variacion_interanual, diferencia_interanual,diferencia_mensual,nombre_prov



"""
#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

lista_provincias = list()
lista_valores_estacionalidad = list() 
lista_valores_sin_estacionalidad = list() 
lista_registro = list()
lista_fechas= list() 

instancia_bdd = conexionBaseDatos(host, user, password, database, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas)
instancia_bdd.conectar_bdd(host, user, password, database)
prueba = instancia_bdd.empleo_otras_nea(18)

print(prueba[0])"""