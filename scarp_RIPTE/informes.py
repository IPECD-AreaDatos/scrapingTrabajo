#Archivo destino a la construccion de informes que se envian por Whatsapp y Gmail.
import calendar
from email.message import EmailMessage
from smtplib import SMTP_SSL
import pandas as pd
from datetime import datetime
from ssl import create_default_context
import ssl
import mysql.connector
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import matplotlib.pyplot as plt
import locale
import smtplib

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

            print(nueva_fecha)
            #Construccion de la cadena de la fecha actual
            nueva_fecha = nueva_fecha.date()
            fecha_cadena = self.obtener_mes_actual(nueva_fecha)
            fecha_mes_anterior = self.obtener_mes_actual(fecha_mes_anterior)
            diciembre_AñoAnterior = self.obtener_mes_actual(diciembre_AñoAnterior)
            fecha_mes_AñoAnterior = self.obtener_mes_actual(fecha_mes_AñoAnterior)
            ruta_archivo_grafico= self.generar_y_guardar_grafico()

            #==== ENVIO DE MENSAJES
            self.enviar_correo(fecha_cadena,nuevo_valor,fecha_mes_anterior,valor_anterior,variacion_mensual,fecha_mes_AñoAnterior,variacion_interanual,diciembre_AñoAnterior,variacion_acumulada, ruta_archivo_grafico)
            

    #Envio de correos por GMAIL
    def enviar_correo(self,fecha_cadena, nuevo_valor,fecha_mes_anterior,valor_anterior,variacion_mensual,fecha_mes_AñoAnterior,variacion_interanual,diciembre_AñoAnterior,variacion_acumulada, ruta_archivo_grafico):
        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com']
        email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com', 'manumarder@gmail.com']
        em = MIMEMultipart()
        asunto = f'Modificación en la base de datos - Remuneración Imponible Promedio de los Trabajadores Estables (RIPTE) - Fecha {fecha_cadena}'
        mensaje = f'''\
            <html>
            <body>
                    <h2 style="font-size: 24px;"><strong> DATOS NUEVOS DE REMUNERACION IMPONIBLE PROMEDIO DE LOS TRABAJADORES ESTABLES (RIPTE) A {fecha_cadena.upper()}. </strong></h2>

            <hr>
            <p>Nueva fecha: {fecha_cadena} -- Nuevo valor:  <span style="font-size: 17px;"><b>${nuevo_valor}<b></p>
            <hr>
            <p>Valor correspondiente a {fecha_mes_anterior}: ${valor_anterior} -- Variación Mensual:  <span style="font-size: 17px;"><b>{variacion_mensual:.2f}%</b>  </p>
            <hr>
            <p>Variación interanual de {fecha_mes_AñoAnterior} a {fecha_cadena}:  <span style="font-size: 17px;"><b>{variacion_interanual:.2f}%</b> </p>
            <hr>
            <p>Variación Acumulada desde {diciembre_AñoAnterior} a {fecha_cadena}:  <span style="font-size: 17px;"><b>{variacion_acumulada:.2f}%</b> </p>
            <hr>
            </body>

            
            Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes <br>
            Dirección: Tucumán 1164 - Corrientes Capital
            - Contacto Coordinación General: 3794 284993
        
            </html>
            '''
        # Establecer el contenido HTML del mensaje
        em.attach(MIMEText(mensaje, 'html'))

        # Adjuntar el gráfico como imagen incrustada
        with open(ruta_archivo_grafico, 'rb') as archivo:
            imagen_adjunta = MIMEImage(archivo.read(), 'png')
            imagen_adjunta.add_header('Content-Disposition', 'attachment', filename='Grafico valores RIPTE en los ultimos 12 meses.png')
            em.attach(imagen_adjunta)

        # Establecer los campos del correo electrónico
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = asunto

        # Crear el contexto SSL
        contexto = ssl.create_default_context()

        # Enviar el correo electrónico
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())

    #Objetivo 
    def obtener_datos(self,nueva_fecha,nuevo_valor):

        #Obtencion de datos de la BDD - Transformacion a DF
        nombre_tabla = 'ripte'
        consulta = f'SELECT * FROM {nombre_tabla}'
        df_bdd = pd.read_sql(consulta,self.conn)


        # ==== CALCULO VARIACION INTERANUAL ==== #


        #Obtencion de la fecha actual - se usara para determinar el valor del año anterior en el mismo mes
        fecha_actual = df_bdd['fecha'].iloc[-1]

        año_actual = fecha_actual.year
        año_anterior = año_actual - 1

        mes_actual = str(fecha_actual.month)
        dia_actual = str(fecha_actual.day)

        #Construccion de la fecha y busqueda del valor
        fecha_mes_AñoAnterior = str(año_anterior)+"-"+mes_actual+"-"+dia_actual
        fecha_mes_AñoAnterior = datetime.strptime(fecha_mes_AñoAnterior,'%Y-%m-%d').date()

        valor_mes_AñoAnterior = df_bdd.loc[df_bdd['fecha'] == fecha_mes_AñoAnterior]
        print(valor_mes_AñoAnterior)
        valor = valor_mes_AñoAnterior['valor'].values[0]

        print(f'NUEVO VALOR:{nuevo_valor} - VALOR ANTERIOR: {valor}')

        #Calculo final
        variacion_interanual = ((nuevo_valor / valor) - 1 ) * 100


        # ===== CALCULO VARIACION ACUMULADA ==== #

        diciembre_AñoAnterior = datetime.strptime(str(año_anterior) + "-" + "12-01",'%Y-%m-%d').date() #--> Fecha de DIC del año anterior

        #SMVM del año anterior
        valor_dic_AñoAnterior = df_bdd.loc[df_bdd['fecha'] == diciembre_AñoAnterior]
        smvm_dic_AñoAnterior = valor_dic_AñoAnterior['valor'].values[0]

        #calculo final
        variacion_acumulada = ((nuevo_valor / smvm_dic_AñoAnterior) - 1) * 100


        #==== CONSTRUCCION DE FECHAS


        
        #Construccion de la fecha del mes anterior al actual --> Para variacion Mensual
        mes_anterior = df_bdd['fecha'].iloc[-2]
        cadena_mes_anterior = str(mes_anterior.year) +"-"+str(mes_anterior.month)


        #Construcion de la fecha del mismo mes , del año pasado --> Para variacion Interanual
        cadena_mes_añoAnterior = str(año_anterior) +"-"+ mes_actual

        #Construccion de la fecha de diciembre del año anterior al ACTUAL --> Para Variacion Acumulada
        cadena_dic_añoAnterior = str(diciembre_AñoAnterior.year) +"-"+str(diciembre_AñoAnterior.month)




        return variacion_interanual, variacion_acumulada,cadena_mes_anterior,cadena_mes_añoAnterior,cadena_dic_añoAnterior

    def obtener_mes_actual(self, fecha_ultimo_registro):
        if isinstance(fecha_ultimo_registro, str):
            fecha_ultimo_registro = datetime.strptime(fecha_ultimo_registro, '%Y-%m').date()
        # Obtener el nombre del mes en inglés
        nombre_mes_ingles = calendar.month_name[fecha_ultimo_registro.month]

        # Diccionario de traducción de meses
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

        # Obtener la traducción del nombre del mes
        nombre_mes_espanol = traducciones_meses.get(nombre_mes_ingles, nombre_mes_ingles)

        # Formatear el resultado
        resultado = f"{nombre_mes_espanol.capitalize()} {fecha_ultimo_registro.year}"
        return resultado
    

    def generar_y_guardar_grafico(self, columna_fecha='fecha', columna_valor='valor', nombre_archivo='variacion_ripte.png'):
        """
        Genera un gráfico de línea a partir de un DataFrame y guarda el resultado en un archivo PNG dentro de la carpeta 'files'.

        Args:
        df (pd.DataFrame): DataFrame que contiene los datos a graficar.
        columna_fecha (str): Nombre de la columna en df que contiene las fechas.
        columna_valor (str): Nombre de la columna en df que contiene los valores a graficar.
        nombre_archivo (str): Nombre del archivo donde se guardará el gráfico.
        """

        #Obtencion de datos de la BDD - Transformacion a DF
        nombre_tabla = 'ripte'
        consulta = f'SELECT * FROM {nombre_tabla} ORDER BY fecha DESC LIMIT 13'
        df = pd.read_sql(consulta,self.conn)

        # Preparación de los datos
        df[columna_fecha] = pd.to_datetime(df[columna_fecha])
        df[columna_valor] = df[columna_valor].astype(int)
    
        # Generación del gráfico
        plt.figure(figsize=(12, 7))
        # Asegúrate de multiplicar df[columna_valor] por 100 aquí para que los valores se muestren como porcentajes
        plt.plot(df[columna_fecha], df[columna_valor], '-o', color='green')  # Multiplicación por 100

        # Establecer configuración regional para el formato de números
        locale.setlocale(locale.LC_ALL, '')

        # Iterar sobre el DataFrame para poner los valores en los puntos
        for i, punto in df.iterrows():
            # Convertir el valor a formato con separadores de miles
            valor_formateado = "{:,}".format(punto[columna_valor])
            # Mostrar el texto formateado
            plt.text(punto[columna_fecha], punto[columna_valor], f'${valor_formateado}', color='black', ha='left', va='bottom')

        plt.title('Valor RIPTE de los ultimos 12 meses')
        plt.xlabel('Fecha')
        plt.ylabel('Valor($)')
        plt.grid(True)

        # Obtener la ruta del directorio actual (donde se encuentra el script)
        directorio_actual = os.path.dirname(os.path.abspath(__file__))

        # Construir la ruta de la carpeta "files" dentro del directorio actual
        carpeta_guardado = os.path.join(directorio_actual, 'files')

        # Asegurarse de que la carpeta "files" exista
        if not os.path.exists(carpeta_guardado):
            os.makedirs(carpeta_guardado)

        # Construir la ruta completa del archivo a guardar
        nombre_archivo_completo = os.path.join(carpeta_guardado, nombre_archivo)

        # Guardar el gráfico
        plt.savefig(nombre_archivo_completo)
        plt.close()
        
        # Devuelve la ruta completa del archivo para su uso posterior
        return nombre_archivo_completo