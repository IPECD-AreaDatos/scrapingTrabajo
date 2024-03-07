from pymysql import connect
import pandas as pd
from calendar import month_name
from email.message import EmailMessage
from ssl import create_default_context
from smtplib import SMTP_SSL
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


    #Objetivo: extraer tabla 'empleo_nacional_porcentajes_variaciones'
    def extract_date_nation(self):

        query = "SELECT * FROM empleo_nacional_porcentajes_variaciones"
        df = pd.read_sql(query,self.conn)

        query_nea = "SELECT * FROM empleo_nea_variaciones"
        df_nea = pd.read_sql(query_nea,self.conn)
        return df,df_nea


    #Objetivo: enviar por correo el informe de SIPA
    def send_mail(self):
        

        #Obtencion de datos
        df,df_nea = self.extract_date_nation()

        #Obtencion de la fecha para el asunto
        fecha_asunto = df['fecha'].iloc[-1]
        fecha_asunto = self.obtener_mes_actual(fecha_asunto) + " del " + str(fecha_asunto.year)

        #Diferencias nacionales
        diferencia_mensual = int((df['empleo_total'].iloc[-1] - df['empleo_total'].iloc[-2]) * 1000)
        diferencia_interanual = int((df['empleo_total'].iloc[-1] - df['empleo_total'].iloc[-13]) * 1000)


        #Mensaje uno, contiene los datos a nivel naciona
        mensaje_uno = f'''
        <html>
        <body>
        <h2>Se ha producido una actualizacion en la base de datos. La tabla de SIPA contiene nuevos datos.</h2>

        <hr>
        <h3> Distribucion de los Trabajos Registrados - Argentina </h3>
        <p>1 - Empleo privados registrados: <span style="font-size: 17px;"><b>{df['p_empleo_privado'].iloc[-1]:,.2f}%</b></span></p>
        <p>2 - Empleos publicos registrados: <span style="font-size: 17px;"><b>{df['p_empleo_publico'].iloc[-1]:,.2f}%</b></span></p>
        <p>3 - Monotributistas Independientes: <span style="font-size: 17px;"><b>{df['p_empleo_independiente_monotributo'].iloc[-1]:,.2f}%</b></span></p>
        <p>4 - Monotributistas Sociales: <span style="font-size: 17px;"><b>{df['p_empleo_monotributo_social'].iloc[-1]:,.2f}%</b></span></p>
        <p>5 - Empleo en casas particulares registrado: <span style="font-size: 17px;"><b>{df['p_empleo_casas_particulares'].iloc[-1]:,.2f}%</b></span></p>
        <p>6 - Trabajadores independientes autonomos: <span style="font-size: 17px;"><b>{df['p_empleo_independiente_autonomo'].iloc[-1]:,.2f}%</b></span></p>
        <hr>
        <h3> Trabajo Registrado a nivel nacional: </h3>
        <p>Total: <span style="font-size: 17px;"><b>{df['empleo_total'].iloc[-1] * 1000 :,.0f}</b></span></p>
        <p>Variacion mensual: <span style="font-size: 17px;"><b>{df['vmensual_empleo_total'].iloc[-1]:,.2f}%</b></span> ({diferencia_mensual:,.0f} puestos)  </p>
        <p>Variacion interanual: <span style="font-size: 17px;"><b>{df['vinter_empleo_total'].iloc[-1]:,.2f}%</b></span>  ({diferencia_interanual:,.0f} puestos)  </p>
        <hr>
        '''


        #=== Mensaje dos, contiene los datos a nivel NEA
        mensaje_dos = f'''
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
                
                
                    {self.difference_by_province(df_nea)}

            </table>
        '''


        #=== Mensaje tres, contiene los datos comparativos del NEA y nacion

        mensaje_tres = f'''
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
                
                
                    {self.difference_nea_nation(df_nea,df)}

            </table>
        '''


        #=== Mensaje cuatro, corresponde a los maximos historicos de nacion y corrientes.
        mensaje_cuatro = self.max_hitory_corr_nation(df_nea,df)

        #Mensaje de presentacion 
        mensaje_cinco = '''
                        <p> Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes<br>
                                 Dirección: Tucumán 1164 - Corrientes Capital<br>
                             Contacto Coordinación General: 3794 284993</p>
                        </body>
                        </html>
                        '''

        #===== SECCION DE ENVIO DE CORREO

        #Concatenacion de cadena
        cadena = mensaje_uno + mensaje_dos + mensaje_tres + mensaje_cuatro + mensaje_cinco

        #Declaramos email desde el que se envia, la contraseña de la api, y los correos receptores.
        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contrasenia = 'cmxddbshnjqfehka'

        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com', 'samaniego18@gmail.com', 'guillermobenasulin@gmail.com', 'leclerc.mauricio@gmail.com']
        email_receptores =  ['benitezeliogaston@gmail.com']

        #==== Zona de envio de correo
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = f'SISTEMA INTEGRADO PREVISIONAL ARGENTINO(SIPA) - Fecha: ({fecha_asunto})'
        em.set_content(cadena, subtype = 'html')

        
        contexto= create_default_context()
        
        with SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contrasenia)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())

    #Objetivo: calculas las diferencias mensuales, interanuales y acumuladas de cada provincia, a la par de crear las filas de la tabla HTML
    def difference_by_province(self,df_nea):

        #Lista con la que iteramos y conseguimos los datos por cada provincia
        lista_provincias = ['corrientes','misiones','chaco','formosa']

        #Contiene los valores de cada fila de las provincias
        cadena_aux = str()

        #Recorremos cada provincia para los calculos
        for provincia in lista_provincias:

            
            #Calculo de diferencias mensuales, interanuales y acumuladas
            dif_m = df_nea['total_'+provincia].iloc[-1] - df_nea['total_'+provincia].iloc[-2]
            dif_i = df_nea['total_'+provincia].iloc[-1] - df_nea['total_'+provincia].iloc[-13]

            #Para la acumulada necesitamos sacar la fecha actual para conseguir diciembre del año anterior
            df_nea['fecha'] = pd.to_datetime(df_nea['fecha'])
            fecha_actual = df_nea['fecha'].iloc[-1]
            dif_a = df_nea['total_'+provincia].iloc[-1] - df_nea['total_'+provincia][ (df_nea['fecha'].dt.year == fecha_actual.year - 1) & (df_nea['fecha'].dt.month == 12)] 
            dif_a = dif_a.values[0] #--> Se genero una "serie", etonces tenemos que extraer el valor de ahi

            #Porcentaje representativo de la provincia en el NEA
            porcentage = ((df_nea['total_'+provincia].iloc[-1] * 100) / df_nea['total_nea'].iloc[-1])


            #=== CONSTRUCCION DE LA CADENA POR PROVINCIAS - recordar que es una fila de tabla de html
            cadena_provincia = f'''
            <tr>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {provincia.capitalize()}</td>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {df_nea['total_'+provincia].iloc[-1]:,.0f}</td>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {df_nea['vmensual_'+provincia].iloc[-1]:,.2f}%</td>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {dif_m:,.0f}</td>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {df_nea['vinter_'+provincia].iloc[-1]:,.2f}%</td>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {dif_i:,.0f}</td>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {df_nea['vacum_'+provincia].iloc[-1]:,.2f}%</td>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {dif_a:,.0f}</td>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {porcentage:,.2f}%</td>
            <tr>
            '''
            cadena_aux = cadena_aux + cadena_provincia


        return cadena_aux


    def difference_nea_nation(self,df_nea,df_nacion):
        


        #=== DATOS DEL NEA
        
        #Creacion de diferencias menuales, interanuales y acumuladas

        dif_m_nea = df_nea['total_nea'].iloc[-1] - df_nea['total_nea'].iloc[-2]
        dif_i_nea = df_nea['total_nea'].iloc[-1] - df_nea['total_nea'].iloc[-13]

        #Para la acumulada necesitamos sacar la fecha actual para conseguir diciembre del año anterior
        df_nea['fecha'] = pd.to_datetime(df_nea['fecha'])
        fecha_actual = df_nea['fecha'].iloc[-1]
        dif_a_nea = df_nea['total_nea'].iloc[-1] - df_nea['total_nea'][ (df_nea['fecha'].dt.year == fecha_actual.year - 1) & (df_nea['fecha'].dt.month == 12)] 
        dif_a_nea = dif_a_nea.values[0] #--> Se genero una "serie", etonces tenemos que extrsaer el valor de ahi

        #Porcentaje representativo de la provincia en el NEA
        porcentage_nea = ((df_nea['total_nea'].iloc[-1] * 100) / df_nacion['empleo_total'].iloc[-1])


        #=== DATOS DE NACION

        dif_m_nacion = int((df_nacion['empleo_total'].iloc[-1] - df_nacion['empleo_total'].iloc[-2]) * 1000)
        dif_i_nacion = int((df_nacion['empleo_total'].iloc[-1] - df_nacion['empleo_total'].iloc[-13]) * 1000)

        #Para la acumulada necesitamos sacar la fecha actual para conseguir diciembre del año anterior
        df_nacion['fecha'] = pd.to_datetime(df_nacion['fecha'])
        fecha_actual = df_nacion['fecha'].iloc[-1]
        dif_a_nacion = df_nacion['empleo_total'].iloc[-1] - df_nacion['empleo_total'][ (df_nacion['fecha'].dt.year == fecha_actual.year - 1) & (df_nacion['fecha'].dt.month == 12)] 
        dif_a_nacion = int(dif_a_nacion.values[0] * 1000 )#--> Se genero una "serie", etonces tenemos que extraer el valor de ahi



        # ==== CONSTRUCCION DEL MENSAJE

        filas_tabla_nea_nacion = f'''<tr>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> NEA </td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {df_nea['total_nea'].iloc[-1]:,.0f}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {df_nea['vmensual_nea'].iloc[-1]:,.2f}%</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {dif_m_nea:,.0f}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {df_nea['vinter_nea'].iloc[-1]:,.2f}%</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {dif_i_nea:,.0f}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {df_nea['vacum_nea'].iloc[-1]:,.2f}%</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {dif_a_nea:,.0f}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {porcentage_nea:,.2f}</td>


        
        <tr>

        <tr>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> NACION </td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {df_nacion['empleo_total'].iloc[-1] * 1000:,.0f}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {df_nacion['vmensual_empleo_total'].iloc[-1]:,.2f}%</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {dif_m_nacion:,.0f}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {df_nacion['vinter_empleo_total'].iloc[-1]:,.2f}%</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {dif_i_nacion:,.0f}</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {df_nacion['vacum_empleo_total'].iloc[-1]:,.2f}%</td>
        <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {dif_a_nacion:,.0f}</td>


        <tr>

        '''

        return filas_tabla_nea_nacion


    #Objetivo: encontrar los maximos historicos de corrientes y de nacion en cuenta del empleo privado
    def max_hitory_corr_nation(self,df_nea,df_nacion):

        #Obtencion del maximo de corrientes
        indice_maximo_nea = df_nea['total_corrientes'].idxmax()
        fila_maximo_nea = df_nea[['fecha','total_corrientes']].loc[indice_maximo_nea]

        #Obtencion del maximo de nacion
        indice_maximo_nacion = df_nacion['empleo_total'].idxmax()
        fila_maximo_nacion = df_nacion[['fecha','empleo_total']].loc[indice_maximo_nacion]

        #Construccion de fechas
        fecha_nacion = str(fila_maximo_nacion['fecha'].month)+"-"+str(fila_maximo_nacion['fecha'].year)
        fecha_corrientes = str(fila_maximo_nea['fecha'].month)+"-"+str(fila_maximo_nea['fecha'].year)

        #Construccion del mensaje
        mensaje_maximos = f'''

        <p> MAXIMO HISTORICO DEL EMPLEO PRIVADO A NIVEL NACIONAL - FECHA {fecha_nacion} - Total: {df_nacion['empleo_total'].values[0] * 1000:,.0f}
        <p> MAXIMO HISTORICO DEL EMPLEO PRIVADO EN CORRIENTES - FECHA {fecha_corrientes} - Total: {df_nea['total_corrientes'].values[0]:,.0f}


        '''

        return mensaje_maximos

    def obtener_mes_actual(self,fecha_ultimo_registro):
        

        # Obtener el nombre del mes actual en inglés
        nombre_mes_ingles = month_name[fecha_ultimo_registro.month]

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

        return nombre_mes_espanol

