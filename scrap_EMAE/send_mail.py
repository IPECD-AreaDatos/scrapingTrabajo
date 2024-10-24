#Archivo destino a la construccion de informes que se envian por Gmail.
import calendar
import pandas as pd
from pymysql import connect
import os
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
import ssl
import smtplib
import numpy as np

class EmailEmae:

    #Declaracion de atributos
    def __init__(self,host,user,password,database):

        self.conn = None
        self.cursor = None
        self.host = host
        self.user = user
        self.password = password
        self.database = database
    
    #Establecemos la conexion a la BDD
    def conectar_bdd(self):
        self.conn = connect(host=self.host, user=self.user, password=self.password, database=self.database)
        self.cursor = self.conn.cursor()


    #Objetivo: construir las rutas de la carpeta del EMAE y de la carpeta de las imagenes del HTML
    def rutas_a_usar(self):

        ruta_archivo = os.path.dirname(os.path.abspath(__file__))
        ruta_folder_images = os.path.join(ruta_archivo, "correo_html_imagenes", "images")

        return ruta_archivo, ruta_folder_images


    #Objetivo: renderizar la plantilla HTML del EMAE
    def get_template(self,ruta_folder_images,ruta_archivo,var_mensual, var_interanual,df_variaciones):

        #Cargamos el entorno, donde esta incluido: la carpeta imagenes y el HTML del correo.
        env = Environment(loader=FileSystemLoader([ruta_folder_images, ruta_archivo]))
        template = env.get_template('correo_html_imagenes/formato_emae.html')

        # Renderizar la plantilla con los parámetros
        html_content = template.render(var_mensual = var_mensual, var_interanual = var_interanual,df_variaciones = df_variaciones)

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
        email_receptores = ["benitezeliogaston@gmail.com"]
        
        # ==== CREACION DEL MENSAJE DE GMAIL ==== #

        # Crear el objeto de mensaje MIME
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f'Actualizacion de datos EMAE - {fecha_asunto}'
        msg['From'] = email_emisor
        msg['To'] = ", ".join(email_receptores)


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


    def variaciones_mensual_interanual_acumulada(self):
        
        #Buscamos los datos de la tabla emae, y lo transformamos a un dataframe
        nombre_tabla = 'emae'
        query_select = f'SELECT * from {nombre_tabla}' 
        df_bdd = pd.read_sql(query_select,self.conn)
        df_bdd['fecha'] = pd.to_datetime(df_bdd['fecha'])#--> Cambiamos formato de la fecha para su manipulacion
        
        #Buscamos los datos de las categorias del emae, para lograr un for con cada indice, y para organizar la tabla por |INDICE|VALOR
        nombre_tabla = 'emae_categoria'
        query_select = f'SELECT * from {nombre_tabla}' 
        df_categorias = pd.read_sql(query_select,self.conn)

        #OBTENCION DEL GRUPO DE LA FECHA MAXIMA 
        fecha_maxima = max(df_bdd['fecha'])

        #LISTAS que acumulan consecutivamente los indices y las variaciones
        lista_indices = []
        lista_var_mensual = []
        lista_var_interanual = []
        lista_var_acumulada = []

        for indice,descripcion_categoria in zip(df_categorias['id_categoria'],df_categorias['categoria_descripcion']):

            #Obtenemos el ultimo valor, de el ID especificado por el valor de variable "indice" --> SE USA EL MISMO VALOR PARA CADA VARIACION
            valor_actual = df_bdd['valor'][df_bdd['sector_productivo'] == indice].values[-1]

            # === CALCULO VARIACION MENSUAL

            valor_mes_anterior = df_bdd['valor'][(df_bdd['sector_productivo'] == indice)].values[-2]

            #Calculo final
            var_mensual = ((valor_actual / valor_mes_anterior) - 1) * 100

            # === CALCULO VARIACION INTERANUAL
            
            valor_ano_anterior = df_bdd['valor'][df_bdd['sector_productivo'] == indice].values[-13]

            #Calculo final
            var_intearnual = ((valor_actual / valor_ano_anterior) - 1) * 100

            # === CALCULO VARIACION ACUMULADA    
            valor_diciembre_ano_anterior = df_bdd['valor'][ (df_bdd['fecha'].dt.year == max(df_bdd['fecha'].dt.year) - 1 ) #--> EL maximo año menos 1 para agarrar el anterior
                                                & (df_bdd['fecha'].dt.month == 12) #--> Mes 12 que corresponde a diciembre
                                                & (df_bdd['sector_productivo'] == indice)].values[0]

            #Calculo final
            var_acumulada = ((valor_actual / valor_diciembre_ano_anterior) - 1) * 100

            #Agregamos cada valor a su correspondiente lista
            lista_indices.append(descripcion_categoria)
            lista_var_mensual.append(var_mensual)
            lista_var_interanual.append(var_intearnual)
            lista_var_acumulada.append(var_acumulada)
            
        #Creacion de DATAFRAME que contiene cada una de las variaciones
        data = {
                'nombre_indices':lista_indices,
                'var_mensual': lista_var_mensual,
                'var_interanual':lista_var_interanual,
                'var_acumulada':lista_var_acumulada
        }

        #Convertimos a DF para manipular
        df = pd.DataFrame(data)
        
         # Ordenar por cada tipo de variación
        df_var_mensual = df.sort_values(by='var_mensual', ascending=False).reset_index(drop=True)
        df_var_interanual = df.sort_values(by='var_interanual', ascending=False).reset_index(drop=True)
        df_var_acumulada = df.sort_values(by='var_acumulada', ascending=False).reset_index(drop=True)


        # Crear un nuevo DataFrame con las categorías y variaciones ordenadas
        df_resultado = pd.DataFrame({
            'categoria_mensual': df_var_mensual['nombre_indices'],
            'var_mensual': df_var_mensual['var_mensual'],
            'categoria_interanual': df_var_interanual['nombre_indices'],
            'var_interanual': df_var_interanual['var_interanual'],
            'categoria_acumulada': df_var_acumulada['nombre_indices'],
            'var_acumulada':df_var_acumulada['var_acumulada']
        })

        # Aplicar truncamiento al DataFrame en las columnas numéricas
        df_resultado[['var_mensual', 'var_interanual', 'var_acumulada']] = df_resultado[
            ['var_mensual', 'var_interanual', 'var_acumulada']
        ].applymap(self.truncar_decimal)

        return df_resultado, fecha_maxima


    # Función para truncar a un decimal
    def truncar_decimal(self,valor):
        return np.floor(valor * 100) / 100
        
    def obtener_fecha_actual(self,fecha_ultimo_registro):

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
    
    def obtener_variacion_anualymensual(self):

        # Buscamos los datos de la tabla emae_variaciones y lo transformamos a un DataFrame
        nombre_tabla = 'emae_variaciones'
        query_select = f'SELECT * FROM {nombre_tabla} ORDER BY fecha DESC LIMIT 1' 
        df_bdd = pd.read_sql(query_select, self.conn)

        #Sacamos los valores del DF y lo asignamos a variables
        var_mensual = self.truncar_decimal(df_bdd['variacion_mensual'].values[0])
        var_interanual = self.truncar_decimal(df_bdd['variacion_interanual'].values[0])
        


        return var_mensual, var_interanual

    #Enviamos los mensajes a cada plataforma
    def main_correo(self):
        
        #Conexion a la bdd
        self.conectar_bdd()

        #Obtenemos las rutas a utilizar
        ruta_archivo, ruta_folder_images = self.rutas_a_usar()

        #Obtencion de datos - POR CATEGORIA DE EMAE
        df_variaciones,fecha_maxima = self.variaciones_mensual_interanual_acumulada()

        #fecha pero en formato texto 
        fecha_maxima = self.obtener_fecha_actual(fecha_maxima)

        #Obtencion de datos a nivel GENERAL DEL EMAE
        var_mensual, var_interanual = self.obtener_variacion_anualymensual()

        # === RENDERIZADO DEL HTML

        #Obtenemos el html renderizado
        html_content = self.get_template(ruta_archivo, ruta_folder_images,var_mensual, var_interanual,df_variaciones)


        self.enviar_correo(ruta_folder_images,fecha_maxima,html_content)


# Cargar las variables de entorno desde el archivo .env
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))

EmailEmae(host_dbb,user_dbb,pass_dbb,dbb_datalake).main_correo()
