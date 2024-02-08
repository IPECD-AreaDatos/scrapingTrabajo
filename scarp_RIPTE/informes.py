#Archivo destino a la construccion de informes que se envian por Whatsapp y Gmail.
import calendar
from email.message import EmailMessage
from smtplib import SMTP_SSL
import pandas as pd
from datetime import datetime
from ssl import create_default_context
from pywhatkit import sendwhatmsg_to_group_instantly
import mysql.connector


class InformesRipte:

    def __init__(self,host,user,password,database):

        #Declaramos las variables
        self.conn = None
        self.cursor = None
        self.host = host
        self.user = user
        self.password = password
        self.database = database

        #Cuando se instancia la clase ya se conecta a la BDD para construir automaticamente los informes
        self.conectar_bdd()

    #Establecemos la conexion a la BDD
    def conectar_bdd(self):
        self.conn = mysql.connector.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self.cursor = self.conn.cursor()

    def enviar_mensajes(self,nueva_fecha, nuevo_valor, valor_anterior):


            #Obtencion de valores para informe por CORREO
            variacion_mensual = ((nuevo_valor / valor_anterior) - 1) * 100
            variacion_interanual, variacion_acumulada, fecha_mes_anterior,fecha_mes_AñoAnterior, diciembre_AñoAnterior = self.obtener_datos(nueva_fecha,nuevo_valor)

            #Construccion de la cadena de la fecha actual
            nueva_fecha = nueva_fecha.date()
            fecha_cadena = self.obtener_mes_actual(nueva_fecha)

            cadena_nueva_fecha = str(nueva_fecha.year) +"-"+str(nueva_fecha.month)

            #==== ENVIO DE MENSAJES
            self.enviar_correo(fecha_cadena,cadena_nueva_fecha,nuevo_valor,fecha_mes_anterior,valor_anterior,variacion_mensual,fecha_mes_AñoAnterior,variacion_interanual,diciembre_AñoAnterior,variacion_acumulada)
            #self.enviar_wpp(cadena_nueva_fecha,nuevo_valor,fecha_mes_anterior,valor_anterior,variacion_mensual,fecha_mes_AñoAnterior,variacion_interanual,diciembre_AñoAnterior,variacion_acumulada)
            

    #Envio de correos por GMAIL
    def enviar_correo(self,fecha_cadena,cadena_nueva_fecha,nuevo_valor,fecha_mes_anterior,valor_anterior,variacion_mensual,fecha_mes_AñoAnterior,variacion_interanual,diciembre_AñoAnterior,variacion_acumulada):
                
        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com']
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com']

        asunto = f'Modificación en la base de datos - Remuneración Imponible Promedio de los Trabajadores Estables (RIPTE) - Fecha {fecha_cadena}'
        mensaje = f'''\
            <html>
            <body>
            <h2>Se ha producido una modificación en la base de datos de RIPTE.</h2>
            <hr>
            <p>Nueva fecha: {cadena_nueva_fecha} -- Nuevo valor:  <span style="font-size: 17px;"><b>${nuevo_valor}<b></p>
            <hr>
            <p>Valor correspondiente a {fecha_mes_anterior}: ${valor_anterior} -- Variación Mensual:  <span style="font-size: 17px;"><b>{variacion_mensual:.2f}%</b>  </p>
            <hr>
            <p>Variación interanual de {cadena_nueva_fecha} a {fecha_mes_AñoAnterior}:  <span style="font-size: 17px;"><b>{variacion_interanual:.2f}%</b> </p>
            <hr>
            <p>Variación Acumulada desde {diciembre_AñoAnterior} a {cadena_nueva_fecha}:  <span style="font-size: 17px;"><b>{variacion_acumulada:.2f}%</b> </p>
            </body>

            
            Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes <br>
            Dirección: Tucumán 1164 - Corrientes Capital
            - Contacto Coordinación General: 3794 284993
        
            </html>
            '''
        
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = asunto
        em.set_content(mensaje, subtype = 'html')
        
        contexto = create_default_context()
        
        with SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())

    def enviar_wpp(self,cadena_nueva_fecha,nuevo_valor,fecha_mes_anterior,valor_anterior,variacion_mensual,fecha_mes_AñoAnterior,variacion_interanual,diciembre_AñoAnterior,variacion_acumulada):

        #Id del grupo de WPP
        id_group = "HLDflq1b7Zn3iT4zNSAIhF"

        # Obtén la hora y los minutos actuales
        now = datetime.now()
        hours = now.hour
        minutes = now.minute + 1  # Suma 1 minuto al tiempo actual


        #Definimos el mensaje
        mensaje = f"""
        *RIPTE*

        - Datos actualizados:
        Ultimo valor: {nuevo_valor}
        Fecha: {cadena_nueva_fecha}

        - Mensual:

        Datos Correspondientes al mes anterior.
        Valor: {valor_anterior}
        Fecha: {fecha_mes_anterior}
        Var. Mensual: {variacion_mensual:.2f}%

        - Interanual:

        Var. interanual de {cadena_nueva_fecha} al {fecha_mes_AñoAnterior}: {variacion_interanual:.2f}%

        - Acumulado:

        Var. Acumulada de {diciembre_AñoAnterior} al {cadena_nueva_fecha}: {variacion_acumulada:.2f}%
        """
    

        # Envía el mensaje programado
        sendwhatmsg_to_group_instantly(id_group, mensaje)

    #Objetivo 
    def obtener_datos(self,nueva_fecha,nuevo_valor):

        #Obtencion de datos de la BDD - Transformacion a DF
        nombre_tabla = 'ripte'
        consulta = f'SELECT * FROM {nombre_tabla}'
        df_bdd = pd.read_sql(consulta,self.conn)


        # ==== CALCULO VARIACION INTERANUAL ==== #


        #Obtencion de la fecha actual - se usara para determinar el valor del año anterior en el mismo mes
        fecha_actual = df_bdd['Fecha'].iloc[-1]

        año_actual = fecha_actual.year
        año_anterior = año_actual - 1

        mes_actual = str(fecha_actual.month)
        dia_actual = str(fecha_actual.day)

        #Construccion de la fecha y busqueda del valor
        fecha_mes_AñoAnterior = str(año_anterior)+"-"+mes_actual+"-"+dia_actual
        fecha_mes_AñoAnterior = datetime.strptime(fecha_mes_AñoAnterior,'%Y-%m-%d').date()

        valor_mes_AñoAnterior = df_bdd.loc[df_bdd['Fecha'] == fecha_mes_AñoAnterior]
        print(valor_mes_AñoAnterior)
        valor = valor_mes_AñoAnterior['ripte'].values[0]

        print(f'NUEVO VALOR:{nuevo_valor} - VALOR ANTERIOR: {valor}')

        #Calculo final
        variacion_interanual = ((nuevo_valor / valor) - 1 ) * 100


        # ===== CALCULO VARIACION ACUMULADA ==== #

        diciembre_AñoAnterior = datetime.strptime(str(año_anterior) + "-" + "12-01",'%Y-%m-%d').date() #--> Fecha de DIC del año anterior

        #SMVM del año anterior
        valor_dic_AñoAnterior = df_bdd.loc[df_bdd['Fecha'] == diciembre_AñoAnterior]
        smvm_dic_AñoAnterior = valor_dic_AñoAnterior['ripte'].values[0]

        #calculo final
        variacion_acumulada = ((nuevo_valor / smvm_dic_AñoAnterior) - 1) * 100


        #==== CONSTRUCCION DE FECHAS


        
        #Construccion de la fecha del mes anterior al actual --> Para variacion Mensual
        mes_anterior = df_bdd['Fecha'].iloc[-2]
        cadena_mes_anterior = str(mes_anterior.year) +"-"+str(mes_anterior.month)


        #Construcion de la fecha del mismo mes , del año pasado --> Para variacion Interanual
        cadena_mes_añoAnterior = str(año_anterior) +"-"+ mes_actual

        #Construccion de la fecha de diciembre del año anterior al ACTUAL --> Para Variacion Acumulada
        cadena_dic_añoAnterior = str(diciembre_AñoAnterior.year) +"-"+str(diciembre_AñoAnterior.month)




        return variacion_interanual, variacion_acumulada,cadena_mes_anterior,cadena_mes_añoAnterior,cadena_dic_añoAnterior

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
