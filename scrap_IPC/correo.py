"""
Archivo destinado a obtener los datos necesarios para el correo del IPC, y el envio del mismo.

Detalles:
    - Al instanciar la clase, ya se genera la conexion a la bdd
"""

from email.message import EmailMessage
from ssl import create_default_context
from smtplib import SMTP_SSL
from calendar import month_name
import pandas as pd
from sqlalchemy import create_engine
from email.mime.image import MIMEImage

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Environment, FileSystemLoader

import os
import ssl

class Correo: 

    #Inicializacion de atributos
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
    
        #Creacion de conexion a la bdd
        self.engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database}")


    #Objetivo: construir las rutas de la carpeta del IPC y de la carpeta de las imagenes del HTML
    def rutas_a_usar(self):

        ruta_archivo = os.path.dirname(os.path.abspath(__file__))
        ruta_folder_images = os.path.join(ruta_archivo, "correo_html_imagenes", "images")

        return ruta_archivo, ruta_folder_images

    
    #Objetivo: traer todos los datos del IPC
    def get_data(self):

        #Construimos la consulta, y obtenemos el DF en base a ella.
        query_select = 'SELECT * FROM ipc_valores'
        df = pd.read_sql(query_select,self.engine)
        return df
    
    #Objetivo: obtener las ultimas variaciones mensuales, interanuales y acumuladas por region. Incluido Nacion
    def get_data_variaciones(self,df,fecha_max):
        
        #Obtenemos las ultimas variaciones a nivel general
        data_variaciones = df[['id_region','var_mensual','var_interanual','var_acumulada']][(df['id_categoria'] == 1) 
                              & (df['id_division'] == 1) 
                              & (df['id_subdivision'] == 1)
                               & (df['fecha'] == fecha_max) ]
        
        #Multiplicamos por cien las variaciones
        data_variaciones[['var_mensual','var_interanual','var_acumulada']] = data_variaciones[['var_mensual','var_interanual','var_acumulada']] * 100
        data_variaciones[['var_mensual','var_interanual','var_acumulada']] = data_variaciones[['var_mensual','var_interanual','var_acumulada']].round(2)

        #Generamos un diccionario para pasar los datos
        dicc_variaciones = {
            'vars_nacion': data_variaciones[['var_mensual','var_interanual','var_acumulada']][data_variaciones['id_region'] == 1].values[0],
            'vars_gba': data_variaciones[['var_mensual','var_interanual','var_acumulada']][data_variaciones['id_region'] == 2].values[0],
            'vars_pampeana': data_variaciones[['var_mensual','var_interanual','var_acumulada']][data_variaciones['id_region'] == 3].values[0],
            'vars_noa': data_variaciones[['var_mensual','var_interanual','var_acumulada']][data_variaciones['id_region'] == 4].values[0],
            'vars_nea': data_variaciones[['var_mensual','var_interanual','var_acumulada']][data_variaciones['id_region'] == 5].values[0],
            'vars_cuyo': data_variaciones[['var_mensual','var_interanual','var_acumulada']][data_variaciones['id_region'] == 6].values[0],
            'vars_patagonia': data_variaciones[['var_mensual','var_interanual','var_acumulada']][data_variaciones['id_region'] == 7].values[0],
           
        }
        return dicc_variaciones

    #Objetivo: renderizar la plantilla HTML del IPC
    def get_template(self,ruta_folder_images,ruta_archivo,dicc_variaciones):

        #Cargamos el entorno, donde esta incluido: la carpeta imagenes y el HTML del correo.
        env = Environment(loader=FileSystemLoader([ruta_folder_images, ruta_archivo]))
        template = env.get_template('correo_html_imagenes/new-email.html')

        # Renderizar la plantilla con los parámetros
        html_content = template.render(dicc_variaciones = dicc_variaciones)

        return html_content
    

    #Objetivo: cargar las rutas de las imagenes en el HTML usando CID
    def get_imagenes(self,ruta_folder_images,html_content,msg):

        # Ruta EXACTAS de imágenes
        ruta_fb = os.path.join(ruta_folder_images, "facebook2x.png")
        ruta_tw = os.path.join(ruta_folder_images, "twitter2x.png")
        ruta_ig = os.path.join(ruta_folder_images, "instagram2x.png")

        # Adjuntar las imágenes AL MENSAJE DEL GMAIL y definir el Content-ID
        with open(ruta_fb, 'rb') as img_fb:
            fb_image = MIMEImage(img_fb.read())
            fb_image.add_header('Content-ID', '<facebook2x>')
            msg.attach(fb_image)

        with open(ruta_tw, 'rb') as img_tw:
            tw_image = MIMEImage(img_tw.read())
            tw_image.add_header('Content-ID', '<twitter2x>')
            msg.attach(tw_image)

        with open(ruta_ig, 'rb') as img_ig:
            ig_image = MIMEImage(img_ig.read())
            ig_image.add_header('Content-ID', '<instagram2x>')
            msg.attach(ig_image)

        # Actualizar el HTML para usar los Content-ID
        html_content = html_content.replace('src="images/facebook2x.png"', 'src="cid:facebook2x"')
        html_content = html_content.replace('src="images/twitter2x.png"', 'src="cid:twitter2x"')
        html_content = html_content.replace('src="images/instagram2x.png"', 'src="cid:instagram2x"')

        return html_content
    
    #Objetivo: finalizar el script enviado un correo.
    def enviar_correo(self,ruta_folder_images,fecha_asunto,html_content):

        # datos del correo
        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        email_receptores = 'benitezeliogaston@gmail.com'

        # ==== CREACION DEL MENSAJE DE GMAIL ==== #

        # Crear el objeto de mensaje MIME
        msg = MIMEMultipart('alternative')
        msg['Subject'] =f'INDICE DE PRECIOS AL CONSUMIDOR (IPC) - {fecha_asunto}'
        msg['From'] = email_emisor
        msg['To'] = email_receptores

        # ==== INCLUCION DE IMAGENES EN EL HTML USANDO CID ==== #

        #Adjuntamos imagenes al contenedor, y retornamos el HTML con las rutas de las imagenes remplazadas
        html_with_images = self.get_imagenes(ruta_folder_images,html_content,msg)

        # ============================================================= #


        # Agregar el contenido HTML al cuerpo del mensaje
        html_part = MIMEText(html_with_images, 'html')
        msg.attach(html_part)

        contexto = ssl.create_default_context()

        # Enviar el correo electrónico
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, msg.as_string())


    #Objetivo: tomar la ultima fecha 
    def formatear_ultima_fecha(self,fecha):
        

        # Obtener el nombre del mes actual en inglés
        nombre_mes_ingles = month_name[fecha.month]

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

        return f"{nombre_mes_espanol} del {fecha.year}"



    def main(self):

        #Obtenemos los datos del ipc
        df = self.get_data()

        #Obtenemos las rutas a utilizar
        ruta_archivo, ruta_folder_images = self.rutas_a_usar()

        # === OBTENCION DEL ASUNTO DEL CORREO

        #Buscamos obtener un formato adecuado para la fecha del asunto
        fecha_max = df['fecha'].max()
        fecha_asunto = self.formatear_ultima_fecha(fecha_max)


        # === OBTENCION DE LOS DATOS PARA EL CORREO

        #Como queremos los ultimos datos, le pasamos la fecha maxima
        dicc_variaciones = self.get_data_variaciones(df,fecha_max)

        # === RENDERIZADO DEL HTML

        #Obtenemos el html renderizado
        html_content = self.get_template(ruta_archivo, ruta_folder_images,dicc_variaciones)

        #DEtalle: al enviar el correo, internamente en la funcion, al generar
        #el mensaje de CORREO, es necesario incluir un apartado de imagenes. 
        self.enviar_correo(ruta_folder_images,fecha_asunto,html_content)



host = '54.94.131.196'
user = 'estadistica'
password = 'Estadistica2024!!'
database = 'datalake_economico'

Correo(host,user,password,database).main()