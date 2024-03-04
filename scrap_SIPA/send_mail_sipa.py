from pymysql import connect
import pandas as pd

class MailSipa:

    def __init__(self,host,user,password,database):

        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
        
    #Conexion a la BDD
    def connect_db(self):

            self.conn = connect(
                host = self.host,
                user = self.user,
                password = self.password,
                database = self.database
            )

            self.cursor = self.conn.cursor()

    def close_conections(self):

        # Confirmar los cambios en la base de datos y cerramos conexiones
        self.conn.commit()
        self.cursor.close()
        self.conn.close()


    #Objetivo: extraer los datos correspondientes al correo de CBT y CBA de la tabla A1
    def extract_date(self):

        query = "SELECT * FROM sipa_valores"
        df = pd.read_sql(query,self.conn)
        return df


def enviar_mensajes(self):

        #Transformador de formato - Transforma una cadena al formato manejado en la region (Argentina)
        locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')

        # === OBTENCION DE TODOS LOS DATOS

        #PORCENTAJES DE EMPLEOS REGISTRADOS
        porcentaje_privado, porcentaje_publico, porcentaje_total_casas_particulares,porcentaje_total_idp_autonomo,porcentaje_total_idp_monotributo,porcentaje_total_idp_monotributo_social,cadena_ultima_fecha = self.obtener_porcentaje_clases()

        #EMPLEO REGISTRADO A NIVEL PAIS - GENERAL
        total_nivel_pais, variacion_mensual, variacion_interanual,diferencia_mensual,diferencia_interanual = self.empleo_registrado_pais()

        #EMPLEO PRIVADO REGISTRADO A NIVEL PAIS 
        total_nivel_pais_privado, variacion_mensual_privado, variacion_interanual_privado, diferencia_mensual_privado,diferencia_interanual_privado,fecha_del_maximo,maximo, promedio_empleo_nacion, variacion_acumulada_privado, diferencia_acumulada_privado = self.empleo_privado_registrado()

        #EMPLEO PRIVADO REGISTRADO EN EL NEA
        total_empleo_nea,variacion_mensual_nea,variacion_interanual_nea, diferencia_interanual_nea,diferencia_mensual_nea,variacion_acumulada_nea,diferencia_acumulada_nea, promedio_empleo_nea = self.empleos_nea()

        #MAXIMO EMPLEO EN CORRIENTES
        fecha_max_corrientes, maximo_corrientes = self.obtener_max_corrientes()

        #Enviamos el correo por GMAIL con todos los datos


        #DATOS DE EMISOR Y RECEPTOR
        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com']
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com']

        self.enviar_correo(
                      cadena_ultima_fecha,
                      porcentaje_privado,
                      porcentaje_publico,
                      porcentaje_total_idp_monotributo,
                      porcentaje_total_idp_monotributo_social,
                      porcentaje_total_casas_particulares,
                      porcentaje_total_idp_autonomo,
                      total_nivel_pais,
                      variacion_mensual,
                      diferencia_mensual,
                      variacion_interanual,
                      diferencia_interanual,
                      total_empleo_nea,
                      variacion_mensual_nea,
                      diferencia_mensual_nea,
                      variacion_interanual_nea,
                      diferencia_interanual_nea,
                      variacion_acumulada_nea,
                      diferencia_acumulada_nea,
                      total_nivel_pais_privado,
                      variacion_mensual_privado,
                      diferencia_mensual_privado,
                      variacion_interanual_privado,
                      diferencia_interanual_privado,
                      variacion_acumulada_privado,
                      diferencia_acumulada_privado,
                      fecha_del_maximo,
                      maximo,
                      fecha_max_corrientes,
                      maximo_corrientes,
                      email_receptores,
                      email_emisor,
                      email_contraseña
                      )

        """        
                        self.enviar_wpp(cadena_ultima_fecha,                      
                        porcentaje_privado,
                            porcentaje_publico,
                            porcentaje_total_idp_monotributo,
                            porcentaje_total_idp_monotributo_social,
                            porcentaje_total_casas_particulares,
                            porcentaje_total_idp_autonomo,                      
                            total_nivel_pais,
                            variacion_mensual,
                            diferencia_mensual,
                            variacion_interanual,
                            diferencia_interanual,
                            total_empleo_nea,                  
                            variacion_mensual_nea,
                            diferencia_mensual_nea,
                            variacion_interanual_nea,
                            diferencia_interanual_nea,
                            variacion_acumulada_nea,
                            diferencia_acumulada_nea,
                            fecha_del_maximo,
                            maximo,)"""

    #Objetivo: enviar por correo el informe de SIPA
    def enviar_correo(self,
                      cadena_ultima_fecha,
                      porcentaje_privado,
                      porcentaje_publico,
                      porcentaje_total_idp_monotributo,
                      porcentaje_total_idp_monotributo_social,
                      porcentaje_total_casas_particulares,
                      porcentaje_total_idp_autonomo,
                      total_nivel_pais,
                      variacion_mensual,
                      diferencia_mensual,
                      variacion_interanual,
                      diferencia_interanual,
                      total_empleo_nea,
                      variacion_mensual_nea,
                      diferencia_mensual_nea,
                      variacion_interanual_nea,
                      diferencia_interanual_nea,
                      variacion_acumulada_nea,
                      diferencia_acumulada_nea,
                      total_nivel_pais_privado,
                      variacion_mensual_privado,
                      diferencia_mensual_privado,
                      variacion_interanual_privado,
                      diferencia_interanual_privado,
                      variacion_acumulada_privado,
                      diferencia_acumulada_privado,
                      fecha_del_maximo,
                      maximo,
                      fecha_max_corrientes,
                      maximo_corrientes,
                      email_receptores,
                      email_emisor,
                      email_contraseña
                      ):
        
        asunto = f'SISTEMA INTEGRADO PREVISIONAL ARGENTINO(SIPA) - Fecha {cadena_ultima_fecha}'

        mensaje_uno = f'''\
        <html>
        <body>
        <h2>Se ha producido una actualizacion en la base de datos. La tabla de SIPA contiene nuevos datos.</h2>

        <hr>
        <h3> Distribucion de los Trabajos Registrados - Argentina </h3>
        <p>1 - Empleo privados registrados: <span style="font-size: 17px;"><b>{porcentaje_privado:,.2f}%</b></span></p>
        <p>2 - Empleos publicos registrados: <span style="font-size: 17px;"><b>{porcentaje_publico:,.2f}%</b></span></p>
        <p>3 - Monotributistas Independientes: <span style="font-size: 17px;"><b>{porcentaje_total_idp_monotributo:,.2f}%</b></span></p>
        <p>4 - Monotributistas Sociales: <span style="font-size: 17px;"><b>{porcentaje_total_idp_monotributo_social:,.2f}%</b></span></p>
        <p>5 - Empleo en casas particulares registrado: <span style="font-size: 17px;"><b>{porcentaje_total_casas_particulares:,.2f}%</b></span></p>
        <p>6 - Trabajadores independientes autonomos: <span style="font-size: 17px;"><b>{porcentaje_total_idp_autonomo:,.2f}%</b></span></p>
        <hr>
        <h3> Trabajo Registrado a nivel nacional: </h3>
        <p>Total: <span style="font-size: 17px;"><b>{"{:n}".format(total_nivel_pais)}</b></span></p>
        <p>Variacion mensual: <span style="font-size: 17px;"><b>{variacion_mensual:,.2f}%</b></span> ({diferencia_mensual} puestos)  </p>
        <p>Variacion interanual: <span style="font-size: 17px;"><b>{variacion_interanual:,.2f}%</b></span>  ({diferencia_interanual} puestos)  </p>
        <hr>
        '''


        #Estas cadenas se usan para empezar y finalizar las tablas de empleo privado
        inicio_tabla = '''
        <h3> TABLA DEL TRABAJO PRIVADO REGISTRADO </h3>
        <table style="border-collapse: collapse; width: 100%;">
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> GRUPO </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> TOTAL EMPLEO </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VARIACION MENSUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> DIFERENCIA MENSUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VARIACION INTERANUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> DIFERENCIA INTERANUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VARIACION ACUMULADA </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> DIFERENCIA ACUMULADA </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> PORCENTAJE REPRESENTATIVO EN EL NEA </th>
        '''
        fin_tabla = '''</table>'''

        #Creacion de datos del NEA - mensaje 2 - Estos datos corresponden a cada provincia en particular del NEA
        for cod_provincia in (18,54,22,34):

            total_empleo_otra,variacion_mensual_otra,variacion_interanual_otra, diferencia_interanual_otra,diferencia_mensual_otra,nombre_prov_otra,variacion_acumulada,diferencia_acumulada,promedio_empleo = self.empleo_otras_nea(cod_provincia)

            #Reprentacion porcentual de la provincia en el NEA
            porcentaje_representativo = ((total_empleo_otra * 100) / total_empleo_nea)


            cadena_aux = f'''
            <tr>
            <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {nombre_prov_otra}</td>
            <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {total_empleo_otra:,}</td>
            <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {variacion_mensual_otra:,.2f}%</td>
            <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {diferencia_mensual_otra:,}</td>
            <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {variacion_interanual_otra:,.2f}%</td>
            <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {diferencia_interanual_otra:,}</td>
            <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {variacion_acumulada:,.2f}%</td>
            <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {diferencia_acumulada:,}</td>
            <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {porcentaje_representativo:,.2f}%</td>


            <tr>
            '''

            inicio_tabla = inicio_tabla + cadena_aux

        
        #Agregamos a la tabla los datos de NACION y del NEA - Respecto al EMPLEO PRIVADO

        porcentaje_nea = ((total_empleo_nea * 100) / total_nivel_pais)

        cadena_nacion_nea = f'''

        <h3> TABLA DEL TRABAJO PRIVADO REGISTRADO NEA </h3>
        <table style="border-collapse: collapse; width: 100%;">
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> GRUPO </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> TOTAL EMPLEO </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VARIACION MENSUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> DIFERENCIA MENSUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VARIACION INTERANUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> DIFERENCIA INTERANUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VARIACION ACUMULADA </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> DIFERENCIA ACUMULADA </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> PORCENTAJE REPRESENTATIVO EN NACION </th>

    
        <tr>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> NEA </td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {total_empleo_nea:,}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {variacion_mensual_nea:,.2f}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {diferencia_mensual_nea:,}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {variacion_interanual_nea:,.2f}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {diferencia_interanual_nea:,}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {variacion_acumulada_nea:,.2f}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {diferencia_acumulada_nea:,}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {porcentaje_nea:,.2f}</td>


        
        <tr>

        <tr>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> NACION </td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {total_nivel_pais_privado:,}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {variacion_mensual_privado:,.2f}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {diferencia_mensual_privado:,}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {variacion_interanual_privado:,.2f}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {diferencia_interanual_privado}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {variacion_acumulada_privado:,.2f}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {diferencia_acumulada_privado:,}%</td>


        <tr>

        '''

        tabla = inicio_tabla + fin_tabla + cadena_nacion_nea + fin_tabla

        comentario_nacion = f'''

        <p> MAXIMO HISTORICO DEL EMPLEO PRIVADO A NIVEL NACIONAL - FECHA {fecha_del_maximo} - Total: {maximo:,}
        <p> MAXIMO HISTORICO DEL EMPLEO PRIVADO EN CORRIENTES - FECHA {fecha_max_corrientes} - Total: {maximo_corrientes:,}


        '''


        mensaje_tres = '''
                        <p> Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes<br>
                                 Dirección: Tucumán 1164 - Corrientes Capital<br>
                             Contacto Coordinación General: 3794 284993</p>
                        </body>
                        </html>
                        '''

        
        mensaje = mensaje_uno + tabla + comentario_nacion + mensaje_tres
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = asunto
        em.set_content(mensaje, subtype = 'html')
        
        contexto= ssl.create_default_context()
        
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())


    def enviar_wpp(self,cadena_ultima_fecha,                      
                   porcentaje_privado,
                    porcentaje_publico,
                    porcentaje_total_idp_monotributo,
                    porcentaje_total_idp_monotributo_social,
                    porcentaje_total_casas_particulares,
                    porcentaje_total_idp_autonomo,                      
                    total_nivel_pais,
                    variacion_mensual,
                    diferencia_mensual,
                    variacion_interanual,
                    diferencia_interanual,
                    total_empleo_nea,                  
                    variacion_mensual_nea,
                    diferencia_mensual_nea,
                    variacion_interanual_nea,
                    diferencia_interanual_nea,
                    variacion_acumulada_nea,
                    diferencia_acumulada_nea,
                    fecha_del_maximo,
                    maximo,):


        # ==== CADENA DE NACION

        cadena_wpp_cabecera = f"""

        *SIPA {cadena_ultima_fecha}*

        *Composicion a nivel nacional:*
        - Empleos privados: {porcentaje_privado:,.1f}%
        - Empleos públicos registrados: {porcentaje_publico:,.1f}%
        - monotributistas independientes: {porcentaje_total_idp_monotributo:,.1f}%
        - Monotributistas sociales: {porcentaje_total_idp_monotributo_social:,.1f}%
        - Empleos en casas particulares registrados: {porcentaje_total_casas_particulares:,.1f}%
        - Trabajadores independientes autónomos: {porcentaje_total_idp_autonomo:,.1f}%

        *Trabajores registrados a nivel Pais:*
        Total: {total_nivel_pais:,} trabajadores
        Variacion mensual: {variacion_mensual:,.1f}%  ({diferencia_mensual:,.1f} puestos) 
        Variacion interanual: {variacion_interanual:,.1f}% ({diferencia_interanual:,.1f} puestos)

        *Maximo historico:*
        FECHA {fecha_del_maximo} - Total: {maximo:,}

        """

        
        porcentaje_nea = ((total_empleo_nea * 100) / total_nivel_pais)

        cadena_datos_nea = f"""
        *DATOS CORRESPONDIENTES A LA TOTALIDAD DEL NEA*
        - Total empleo: {total_empleo_nea:,} trabajadores
        - Variacion mensual: {variacion_mensual_nea:,.1f}% ({diferencia_mensual_nea:,} puestos)
        - Variacion interanual: {variacion_interanual_nea:,.1f}% ({diferencia_interanual_nea:,} puestos)
        - Variacion acumulada: {variacion_acumulada_nea:,.1f}% ({diferencia_acumulada_nea:,} puestos)
        - Porcentaje que representa el NEA en el trabajo privado de NACION: {porcentaje_nea:,.1f}%
        """


        # === CADENA DEL NEA
        cadena_datos_nea_por_provincia = f"""
        *NEA:*

        """


        lista_id_provincias = [18,54,22,34]
         #Creacion de datos del NEA - mensaje 2 - Estos datos corresponden a cada provincia en particular del NEA
        for cod_provincia in lista_id_provincias:

            total_empleo_otra,variacion_mensual_otra,variacion_interanual_otra, diferencia_interanual_otra,diferencia_mensual_otra,nombre_prov_otra,variacion_acumulada,diferencia_acumulada,promedio_empleo = self.empleo_otras_nea(cod_provincia)

            #Reprentacion porcentual de la provincia en el NEA
            porcentaje_representativo = ((total_empleo_otra * 100) / total_empleo_nea)
            fecha_del_maximo,maximo = self.obtener_max_por_provincia(cod_provincia)
        
            cadena_aux = f'''
            * *{nombre_prov_otra}*
            - Total empleo:{total_empleo_otra:,} trabajadores
            - Variacion mensual: {variacion_mensual_otra:,.1f}% ({diferencia_mensual_otra:,} puestos)
            - Variacion interanual: {variacion_interanual_otra:,.1f}% ( {diferencia_interanual_otra:,} puestos)
            - Variacion Acumulada: {variacion_acumulada:,.1f}% ({diferencia_acumulada:,} puestos)
            - Porcentaje que representa en el NEA: {porcentaje_representativo:,.2f}%
            - Maximo historico del empleo privado: {maximo:,} trabajadores - Fecha: {fecha_del_maximo}
            '''

            cadena_datos_nea_por_provincia = cadena_datos_nea_por_provincia + cadena_aux

        cadena_final = cadena_wpp_cabecera + cadena_datos_nea + cadena_datos_nea_por_provincia 


        #==== Enviamos a grupo de wpp

        #Id del grupo
        id_group = "HLDflq1b7Zn3iT4zNSAIhF"


        # Obtén la hora y los minutos actuales
        now = datetime.datetime.now()
        hours = now.hour
        minutes = now.minute + 1  # Suma 1 minuto al tiempo actual

        # Envía el mensaje programado
        kit.sendwhatmsg_to_group_instantly(id_group, cadena_final)







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
        
        cadena_ultima_fecha = self.obtener_mes_actual(ultima_fecha)
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

        #=== Calculo de la VARIACION ACUMULADA 
        grupo_dic_año_anterior =  df_bdd[(df_bdd['Fecha'].dt.year == fecha_max.year-1 ) & (df_bdd['Fecha'].dt.month == 12)]
        total_dic_año_anterior = sum(grupo_dic_año_anterior['Cantidad_con_Estacionalidad']) * 1000
        variacion_acumulada = ((total_nivel_pais / total_dic_año_anterior) - 1) * 100

        #DIFERENCIAS de cantidades en terminos mensuales, interanual y acumulada
        diferencia_mensual = int(total_nivel_pais - total_mes_anterior)
        diferencia_interanual = int(total_nivel_pais - total_nea_año_anterior)
        diferencia_acumulada = int(total_nivel_pais - total_dic_año_anterior) 

        #=== Obtencion del registro de valor maximo registrado
        
        #indice del maximo
        indice_maximo = df_bdd['Cantidad_con_Estacionalidad'].idxmax()
        fila=df_bdd.loc[indice_maximo] 

        fecha_del_maximo = fila['Fecha']
        maximo = int(fila['Cantidad_con_Estacionalidad'] * 1000)

        fecha_del_maximo = str(fecha_del_maximo.year) + "-" + str(fecha_del_maximo.month)

        #=== Calculo del PROMEDIO de empleo DEL AÑO ACTUAL
        grupo_año_actual = df_bdd[(df_bdd['Fecha'].dt.year == fecha_max.year )]
        promedio_empleo = (int(sum(grupo_año_actual['Cantidad_con_Estacionalidad'])) / len(grupo_año_actual['Cantidad_con_Estacionalidad'])) * 1000



        return total_nivel_pais, variacion_mensual, variacion_interanual, diferencia_mensual,diferencia_interanual, fecha_del_maximo,maximo,promedio_empleo, variacion_acumulada, diferencia_acumulada
     
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
        
        total_empleo = int(sum(grupo_ultima_fecha['Cantidad_con_Estacionalidad']) * 1000)

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


        #=== Calculo de la VARIACION ACUMULADA
        grupo_dic_año_anterior = df_bdd[(df_bdd['Fecha'].dt.year == fecha_max.year-1 ) & (df_bdd['Fecha'].dt.month == 12)]
        total_dic_año_anterior = sum(grupo_dic_año_anterior['Cantidad_con_Estacionalidad']) * 1000
        variacion_acumulada = ((total_empleo / total_dic_año_anterior) - 1) * 100


        #Calculos de diferencias por MES y AÑO
        diferencia_mensual = int(total_empleo - total_mes_anterior)
        diferencia_interanual = int(total_empleo - total_nea_año_anterior)
        diferencia_acumulada  = int(total_empleo - variacion_acumulada)

        #=== Calculo del PROMEDIO de empleo DEL AÑO ACTUAL
        grupo_año_actual = df_bdd[(df_bdd['Fecha'].dt.year == fecha_max.year )]
        promedio_empleo = (int(sum(grupo_año_actual['Cantidad_con_Estacionalidad'])) / len(grupo_año_actual['Cantidad_con_Estacionalidad'])) * 1000


        return total_empleo,variacion_mensual,variacion_interanual, diferencia_interanual,diferencia_mensual, variacion_acumulada,diferencia_acumulada ,promedio_empleo


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
        
        total_empleo = int(grupo_ultima_fecha['Cantidad_con_Estacionalidad'] * 1000)

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

        #=== Calculo de la VARIACION ACUMULADA
        grupo_diciembre_año_anterior = df_bdd[(df_bdd['Fecha'].dt.year == fecha_max.year-1 ) & (df_bdd['Fecha'].dt.month == 12)]
        total_dic_año_anterior = sum(grupo_diciembre_año_anterior['Cantidad_con_Estacionalidad']) * 1000
        variacion_acumulada = ((total_empleo/total_dic_año_anterior) - 1) * 100

        #Calculos de diferencias por MES y AÑO
        diferencia_mensual = int(total_empleo - total_mes_anterior)
        diferencia_interanual = int(total_empleo - total_nea_año_anterior)
        diferencia_acumulada = int(total_empleo-total_dic_año_anterior)


         #=== Calculo del PROMEDIO de empleo DEL AÑO ACTUAL
        grupo_año_actual = df_bdd[(df_bdd['Fecha'].dt.year == fecha_max.year )]
        promedio_empleo = (int(sum(grupo_año_actual['Cantidad_con_Estacionalidad'])) / len(grupo_año_actual['Cantidad_con_Estacionalidad'])) * 1000


        return total_empleo,variacion_mensual,variacion_interanual, diferencia_interanual,diferencia_mensual,nombre_prov,variacion_acumulada,diferencia_acumulada,promedio_empleo
    
    #Buscamos obtener que porcetaje representa la pronvincia en cuestion en el nea
    def porcentaje_representativo_nea(self):
        
        #No hacemos referencia al tipo de registro porque los datos ya estan guardados como PRIVADO
        nombre_tabla = 'sipa_registro'
        nombre_tabla_provincias = 'dp_provincias'

        query_consulta = f'SELECT * FROM {nombre_tabla}'
        query_nombre_prov = f'SELECT nombre_provincia_indec FROM {nombre_tabla_provincias}'        


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





    def obtener_max_corrientes(self):

        nombre_tabla = 'sipa_registro'
        query_consulta = f'SELECT * FROM {nombre_tabla} WHERE ID_Provincia = 18'
        df_bdd = pd.read_sql(query_consulta,self.conn)

        #=== Obtencion del registro de valor maximo registrado
        
        #indice del maximo
        indice_maximo = df_bdd['Cantidad_con_Estacionalidad'].idxmax()
        fila=df_bdd.loc[indice_maximo] 

        fecha_del_maximo = fila['Fecha']
        maximo = int(fila['Cantidad_con_Estacionalidad'] * 1000)

        fecha_del_maximo = str(fecha_del_maximo.year) + "-" + str(fecha_del_maximo.month)

        return fecha_del_maximo,maximo


    def obtener_max_por_provincia(self,id_provincia):

        nombre_tabla = 'sipa_registro'
        query_consulta = f'SELECT * FROM {nombre_tabla} WHERE ID_Provincia = {id_provincia}'
        df_bdd = pd.read_sql(query_consulta,self.conn)

        #=== Obtencion del registro de valor maximo registrado
        
        #indice del maximo
        indice_maximo = df_bdd['Cantidad_con_Estacionalidad'].idxmax()
        fila=df_bdd.loc[indice_maximo] 

        fecha_del_maximo = fila['Fecha']
        maximo = int(fila['Cantidad_con_Estacionalidad'] * 1000)

        fecha_del_maximo = str(fecha_del_maximo.year) + "-" + str(fecha_del_maximo.month)

        return fecha_del_maximo,maximo



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
