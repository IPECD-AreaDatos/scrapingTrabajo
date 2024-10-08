import pandas as pd
from email.message import EmailMessage
import ssl
import smtplib
from calendar import month_name
import pymysql
import os
import matplotlib.pyplot as plt

class MailCBTCBA:

    def __init__(self,host,user,password,database):

        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None
        
    #Conexion a la BDD
    def connect_db(self):

            self.conn = pymysql.connect(
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

        query = "SELECT * FROM correo_cbt_cba"
        df = pd.read_sql(query,self.conn)
        return df


    #Objetivo: enviar los correos de CBT y CBA. Le pasamos como parametro 'conn' que es la conexion a la BDD.
    def send_mail_cbt_cba(self):


        #Conectar a la BDD
        self.connect_db()

        #Extracion de datos
        df = self.extract_date()
        #Asunto del correo - Tomamos la ultima fecha y la transformamos en un formato normal
        fecha_actual = df['fecha'].iloc[-1]
        mes_actual = self.fecha_formato_legible(fecha_actual) #--> Usada para el asunto
        cadena_fecha_actual = mes_actual + " del " + str(fecha_actual.year)

        #Mes anterior
        mes_anterior = df['fecha'].iloc[-2]
        mes_anterior = self.fecha_formato_legible(mes_anterior)
     

        #Construccion del mensaje
        mensaje_uno = f"""        
        <html> 
        <body>


        <h2 style="font-size: 24px;"><strong> DATOS CORRESPONDIENTES AL NORDESTE ARGENTINO (NEA) {cadena_fecha_actual.upper()}. </strong></h2>
   
        <br>

        <p> Este correo contiene información respecto a <b>CBA</b> (Canasta Básica Alimentaria) y <b>CBT</b> (Canasta Básica Total).  </p>
        <hr>

        <p>
        🗓️ En <span style="font-weight: bold;">{cadena_fecha_actual}</span>, en el NEA una persona necesitó
        <span style="font-size: 17px; font-weight: bold;">${df['cba_nea'].iloc[-1]:,.2f}</span> para no ser
        <b>indigente</b> y
        <span style="font-size: 17px; font-weight: bold;">${df['cbt_nea'].iloc[-1]:,.2f}</span> para no ser
        <b>pobre.</b>
        </p>

        


        <p> 👥👥 Una familia tipo (compuesta por 4 integrantes) necesitó de 
        <span style="font-size: 17px;"><b>${df['cba_nea_familia'].iloc[-1]:,.2f}</b></span> para no ser indigente y
        <span style="font-size: 17px;"><b>${df['cbt_nea_familia'].iloc[-1]:,.2f}</b></span> para no ser pobre.
        </p> 
        <p> 👥👥En {mes_anterior}, una misma familia había necesitado 
        <span style="font-size: 17px;"><b>${df['cba_nea_familia'].iloc[-2]:,.2f}</b></span> para no ser indigente y 
        <span style="font-size: 17px;"><b>${df['cbt_nea_familia'].iloc[-2]:,.2f}</b></span> para no ser pobre.
        </p> 

        <hr>

        """


        # Verificar si var_interanual_cba es negativa o positiva
        if df['vinter_cba'].iloc[-1] < 0:
            aumento_disminucion = "disminuyó ⬇️"
            emoji_aumento_disminucion = "📉"
        else:
            aumento_disminucion = "aumentó ⬆️"
            emoji_aumento_disminucion = "📈"
        
        if df['vinter_cbt'].iloc[-1] < 0:
            aumento_canastaBasica = "disminuyó ⬇️"
            emoji_aumento_disminucion = "📉"
        else:
            aumento_canastaBasica = "aumentó ⬆️"
            emoji_aumento_disminucion = "📈"

        if df['vmensual_cba'].iloc[-1] < 0:
            aumento_mensual_canastaBasica= "disminuyó ⬇️"
            emoji_aumento_disminucion = "📉"
        else:
            aumento_mensual_canastaBasica = "aumentó ⬆️"
            emoji_aumento_disminucion = "📈"

        if df['vmensual_cbt'].iloc[-1] < 0:
            aumento_mensual_canastaTotal=  "disminuyó ⬇️"
            emoji_aumento_disminucion = "📉"
        else:
            aumento_mensual_canastaTotal = "aumentó ⬆️"
            emoji_aumento_disminucion = "📈"


        #Fechas a manejar
        fecha_var_inter = df['fecha'].iloc[-13]

        mes_var_inter = self.fecha_formato_legible(fecha_var_inter)
        cadena_var_inter = mes_var_inter + " del " + str(fecha_var_inter.year)

        cadena_mes_anterior = mes_anterior + " del " + str(fecha_actual.year)

    
        mensaje_dos = f"""
        <p>
        {emoji_aumento_disminucion}La canasta básica alimentaria <span style="font-weight: bold; text-decoration: underline;">{aumento_disminucion}</span> interanualmente ({cadena_fecha_actual.upper()} VS {cadena_var_inter.upper()}) un 
        <span style="font-size: 17px; font-weight: bold;">{abs( df['vinter_cba'].iloc[-1]*100):.2f}%</span>
        mientras que la canasta básica total <span style="font-weight: bold; text-decoration: underline;">{aumento_canastaBasica}</span> para el mismo periodo un 
        <span style="font-size: 17px; font-weight: bold;">{ df['vinter_cbt'].iloc[-1]*100:.2f}%</span>.
        </p>
        <p>
        {emoji_aumento_disminucion}La canasta básica alimentaria <span style="font-weight: bold; text-decoration: underline;">{aumento_mensual_canastaBasica}</span> mensualmente ({cadena_fecha_actual.upper()} VS {cadena_mes_anterior.upper()}) un 
        <span style="font-size: 17px; font-weight: bold;">{df['vmensual_cba'].iloc[-1]*100:.2f}%</span>
        mientras que la canasta básica total <span style="font-weight: bold; text-decoration: underline;">{aumento_mensual_canastaTotal}</span> para el mismo periodo un 
        <span style="font-size: 17px; font-weight: bold;">{df['vmensual_cbt'].iloc[-1]*100:.2f}%</span>.
        </p>
        <hr>
        """

        # Verificar si las variaciones del IPC no son None
        cadena_variaciones = ""
        if pd.notna(df['vmensual_nea_ipc'].iloc[-1]) and pd.notna(df['vinter_nea_ipc'].iloc[-1]):
            try:
                var_mensual_ipc = float(df['vmensual_nea_ipc'].iloc[-1])
                var_interanual_ipc = float(df['vinter_nea_ipc'].iloc[-1])
                cadena_variaciones = f"""
                <p>
                📊Respecto al Índice de Precios al Consumidor del NEA, para el mes de {cadena_fecha_actual.upper()} la variación general de precios respecto a {cadena_mes_anterior.upper()} fue de 
                <span style="font-size: 17px;"><b>{var_mensual_ipc*100:.2f}%</b></span>. La variación interanual fue de <span style="font-size: 17px;"><b>{var_interanual_ipc*100:.2f}%</b></span>
                ({cadena_fecha_actual.upper()} VS {cadena_var_inter.upper()})
                </p>
                <hr>
                """
            except ValueError:
                pass
        #===== SECCION DE ENVIO DE CORREO =====
        # Concatenación del mensaje final con el footer siempre presente
        cadena = mensaje_uno + mensaje_dos + cadena_variaciones + """
        <p> Instituto Provincial de Estadística y Ciencia de Datos de Corrientes<br>
        Dirección: Tucumán 1164 - Corrientes Capital<br>
        Contacto Coordinación General: 3794 284993</p>
        </body>
        </html>
        """

        #Declaramos email desde el que se envia, la contraseña de la api, y los correos receptores.
        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contrasenia = 'cmxddbshnjqfehka'

        email_receptores =  ['samaniego18@gmail.com','benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com', 'samaniego18@gmail.com', 'guillermobenasulin@gmail.com', 'leclerc.mauricio@gmail.com','alejandrobrunel@gmail.com']
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com', 'manumarder@gmail.com']
        #email_receptores =  ['benitezeliogaston@gmail.com']

        #==== Zona de envio de correo
        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = f"CBT y CBA - Actualizacion - Fecha: {cadena_fecha_actual}"
        em.set_content(cadena, subtype = 'html')

        contexto = ssl.create_default_context()

        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contrasenia)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())


    #Objetivo: obtener una cadena de fecha en formato legible
    def fecha_formato_legible(self,fecha_ultimo_registro):

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

        #Retornamos una cadena con la fecha en espanol en un formato legible
        return nombre_mes_espanol
    
    def graficos_vars_mensuales_interanuales(self,df):


        df['fecha'] = pd.to_datetime(df['fecha'])

        #=== GENERACION DE GRAFICO
        plt.figure(figsize=(10,10))

        #Grafico CBT
        plt.plot(df['fecha'],df['vmensual_cbt'], color = 'orange',alpha = 0.5, markersize=5)
        plt.plot(df['fecha'],df['vmensual_cbt'],'.',color = "red")

    #Agregar valores de Y a cada punto del gráfico
        for x, y in zip(df['fecha'], df['vmensual_cbt']):
            plt.text(x, y, f'{y:.2f}', ha='right', va='bottom')  # Truncar a dos decimales


        plt.grid()

        plt.title("Variaciones mensuales de CBA")


        #=== DIRECCIONES DE GUARDADO

        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        # Asegurarse de que la carpeta "files" exista
        if not os.path.exists(carpeta_guardado):
            os.makedirs(carpeta_guardado)


        nombre_grafico = "vars_mensuales"

        # Construir la ruta completa del archivo a guardar
        nombre_archivo_completo = os.path.join(carpeta_guardado, nombre_grafico)

        # Guardar el gráfico
        plt.savefig(nombre_archivo_completo)
        plt.close()


