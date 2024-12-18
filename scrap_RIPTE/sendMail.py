#Archivo destino a la construccion de informes que se envian por Whatsapp y Gmail.
import calendar
from email.message import EmailMessage
from smtplib import SMTP_SSL
import pandas as pd
from datetime import datetime
from ssl import create_default_context
import ssl
import pymysql
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import matplotlib.pyplot as plt
import locale
import smtplib

class InformeRipte:

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
        self.conn = pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self.cursor = self.conn.cursor()

    def obtener_correos(self):
        conn = pymysql.connect(
                host = self.host,
                user = self.user,
                password = self.password,
                database = 'ipecd_economico'
            )

        cursor = conn.cursor()

        # Consulta para obtener los correos
        #consulta = "SELECT email FROM correos WHERE prueba = 1"
        consulta = "SELECT email FROM correos"
        
        cursor.execute(consulta)
        correos = cursor.fetchall()

        # Convertir tuplas a lista de strings
        email_receptores = [correo[0] for correo in correos]

        # Cerrar la conexión
        conn.close()

        return email_receptores

    def enviar_mensajes(self,nueva_fecha, nuevo_valor, valor_anterior):
            # Calcular la variación mensual
            variacion_mensual = ((nuevo_valor / valor_anterior) - 1) * 100

            variacion_interanual, variacion_acumulada, fecha_mes_anterior,fecha_mes_AñoAnterior, diciembre_AñoAnterior = self.obtener_datos(nueva_fecha,nuevo_valor)
                
            valor_dolarizado = self.dolarizar(nuevo_valor, nueva_fecha)
            # Obtener la fecha y hora actuales
            fecha_actual = datetime.now()
            dia_actual = fecha_actual.day
            print(nueva_fecha)
            #Construccion de la cadena de la fecha actual
            nueva_fecha = nueva_fecha.date()
            fecha_cadena = self.obtener_mes_actual(nueva_fecha)
            fecha_mes_anterior = self.obtener_mes_actual(fecha_mes_anterior)
            diciembre_AñoAnterior = self.obtener_mes_actual(diciembre_AñoAnterior)
            fecha_mes_AñoAnterior = self.obtener_mes_actual(fecha_mes_AñoAnterior)
            ruta_archivo_grafico= self.generar_y_guardar_grafico()

            #==== ENVIO DE MENSAJES
            self.enviar_correo(fecha_cadena,dia_actual ,nuevo_valor,valor_dolarizado,fecha_mes_anterior,valor_anterior,variacion_mensual,fecha_mes_AñoAnterior,variacion_interanual,diciembre_AñoAnterior,variacion_acumulada, ruta_archivo_grafico)
            

    #Envio de correos por GMAIL
    def enviar_correo(self,fecha_cadena, dia_actual, nuevo_valor, valor_dolarizado,fecha_mes_anterior,valor_anterior,variacion_mensual,fecha_mes_AñoAnterior,variacion_interanual,diciembre_AñoAnterior,variacion_acumulada, ruta_archivo_grafico):
        nuevo_valor_formateado = f"{nuevo_valor:,}".replace(',', '.')
        valor_dolarizado_formateado = f"{valor_dolarizado:,.2f}".replace(',', '.').replace('.', ',', 1)

        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'

        email_receptores = self.obtener_correos()

        em = MIMEMultipart()
        asunto = f'Modificación en la base de datos - Remuneración Imponible Promedio de los Trabajadores Estables (RIPTE) - Fecha {fecha_cadena}'
        mensaje = f'''\
            <html>
            <head>
                <style>
                    .
                    .box h4 {{
                        font-size: 20px;
                        color: white;
                        margin-bottom: 10px;
                        text-transform: uppercase;
                        font-weight: 200;

                    }}
                    .box p {{
                        font-size: 24px;
                        color: white;
                        font-weight: bold;
                    }}
                    </style>
                </head>
                <body>
                <div class="container" style= "background-color: #ffffff; background-image: url('cid:fondo'); background-repeat: no-repeat; background-position: center center; background-size: cover;">
                    <h2 style="font-size: 24px; color: #444; text-align: center;"><strong>DATOS NUEVOS DE REMUNERACION IMPONIBLE PROMEDIO DE LOS TRABAJADORES ESTABLES (RIPTE) A {fecha_cadena.upper()}</strong></h2>
                    <h3 style="font-size: 15px; color: #666; font-weight: 100; text-align: center;">RIPTE es un importante indicador salarial de naturaleza previsional, elaborado por la Subsecretaría de Seguridad Social. Este índice mide la remuneración promedio sujeta 
                    a aportes al Sistema Integrado Previsional Argentino (SIPA) de los trabajadores en relación de dependencia, tanto del sector público como privado.</h3>  
                    <div class="container-valores" style="font-size: 36px;font-weight: bold;color: #333;margin: 10px 0; text-align: center;">
                        <img src="cid:icono_ripte" alt="Icono" style="width: 200px; height: 190px;pointer-events: none; user-select: none;">
                        <br>
                        AR${nuevo_valor_formateado}
                        <br> 
                        US${valor_dolarizado_formateado}*
                        <span class="leyenda" style="font-size: 14px; font-weight: 100;color: #666; display: block;">*Valor calculado en base al dólar blue cotización al {dia_actual} de {fecha_cadena}</span>
                    </div>
                    <div class="container-variaciones" style="width: 100%; display: flex; justify-content: center; align-items: center; flex-wrap: wrap;">
                        <div class="box" style="background-color: #e86900; border-radius: 10px; padding: 10px; margin: 5px; text-align: center; flex: 1 1 100%; max-width: 300px;">
                            <h4 style="font-size: 20px; color: white; margin-bottom: 10px;">Variación Mensual</h4>
                            <p style="font-size: 24px; color: white;">{variacion_mensual:.1f}%</p>
                        </div>
                        <div class="box" style="background-color: #e86900; border-radius: 10px; padding: 10px; margin: 5px; text-align: center; flex: 1 1 100%; max-width: 300px;">
                            <h4 style="font-size: 20px; color: white; margin-bottom: 10px;">Variación Interanual</h4>
                            <p style="font-size: 24px; color: white;">{variacion_interanual:.1f}%</p>
                        </div>
                        <div class="box" style="background-color: #e86900; border-radius: 10px; padding: 10px; margin: 5px; text-align: center; flex: 1 1 100%; max-width: 300px;">
                            <h4 style="font-size: 20px; color: white; margin-bottom: 10px;">Variación Acumulada</h4>
                            <p style="font-size: 24px; color: white;">{variacion_acumulada:.1f}%</p>
                        </div>
                    </div>
                    <div class="footer" style="font-size: 15px; color: #888; text-align: center" >
                        <img src="cid:ipecd" alt="IPI Image" style="margin-right: 20px; max-width: 250px; height: auto; pointer-events: none; user-select: none;" >
                    </div>
                </div>
            </body>    
            </html>
            '''
        # Establecer el contenido HTML del mensaje
        em.attach(MIMEText(mensaje, 'html'))

        # Obtener el directorio actual donde se encuentra el script
        script_dir = os.path.dirname(__file__)

        # Definir la carpeta donde se encuentran las imágenes
        image_dir = os.path.join(script_dir, 'files')

        # Diccionario de nombres de archivos de imágenes
        image_files = {
            "ipecd": "logo_ipecd.png", 
            "fondo": "fondo_correo.png",
            "icono_ripte": "datos_empleo_icono.png"
        }

        # Construir las rutas completas y crear un diccionario para las rutas de las imágenes
        image_paths = {cid: os.path.join(image_dir, filename) for cid, filename in image_files.items()}

        # Adjuntar las imágenes
        for cid, path in image_paths.items():
            with open(path, 'rb') as img_file:
                img = MIMEImage(img_file.read())
                img.add_header('Content-ID', f'<{cid}>')
                em.attach(img)


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

    def dolarizar(self, valor, fecha):
        # Convertir la fecha a un formato compatible con la consulta SQL (Año-Mes)
        fecha_formateada = fecha.strftime('%Y-%m')

        # Consulta SQL para obtener el último valor del dólar blue en el mes de la fecha proporcionada
        consulta = f'''
        SELECT venta FROM dolar_blue
        WHERE DATE_FORMAT(fecha, '%Y-%m') = '{fecha_formateada}'
        ORDER BY fecha DESC
        LIMIT 1;
        '''
        self.cursor.execute(consulta)
        resultado = self.cursor.fetchone()
        if resultado:
            valor_dolar = resultado[0]
            valor_dolarizado = round(valor/ valor_dolar, 2)
            return valor_dolarizado
        else:
            raise ValueError("No se encontró un valor de dólar para la fecha proporcionada.")

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
        valor = valor_mes_AñoAnterior['valor'].values[0]

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