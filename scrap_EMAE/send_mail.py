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
from sqlalchemy import create_engine

load_dotenv()

class EmailEmae:

    #Declaracion de atributos
    def __init__(self,host,user,password,database):

        self.conn = None
        self.cursor = None
        self.host = host
        self.user = user
        self.password = password

        #Se carga la var del datalake economico
        self.database = database

        #Nombre de ipecd_economico
        self.database_correo = (os.getenv('NAME_IPECD_ECONOMICO'))

        #Var de ipecd economico
    
    #Establecemos la conexion a la BDD
    def conectar_bdd(self):

        #Conectamos al datalake para los datos del informe
        self.conn = connect(host=self.host, user=self.user, password=self.password, database=self.database)
        


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
        #email_receptores = self.obtener_correos()
        email_receptores =  ['matizalazar2001@gmail.com', 'manumarder@gmail.com']

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


    def obtener_correos(self):

        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{3306}/{self.database_correo}")

        #query = "SELECT email FROM correos WHERE prueba = 1"
        query = "SELECT email FROM correos"
        correos = pd.read_sql(query,engine).values

        # Convertir tuplas a lista de strings
        email_receptores = [correo[0] for correo in correos]

        print(email_receptores)

        return email_receptores
        

    def variaciones_mensual_interanual_acumulada(self):

        # Usar SQLAlchemy engine para evitar warnings y tener compatibilidad completa
        engine = create_engine(f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}")
        
        # Obtener datos principales
        df_bdd = pd.read_sql("SELECT * FROM emae", engine)
        df_categorias = pd.read_sql("SELECT * FROM emae_categoria", engine)

        if df_bdd.empty or df_categorias.empty:
            print("❌ No hay datos en la tabla EMAE o en emae_categoria.")
            return pd.DataFrame(), None

        df_bdd['fecha'] = pd.to_datetime(df_bdd['fecha'])

        # Validación global de mínimo de registros por serie
        if len(df_bdd) < 13:
            print("⚠️ No hay suficientes datos históricos para calcular variaciones anuales.")
            return pd.DataFrame(), None

        fecha_maxima = df_bdd['fecha'].max()

        lista_indices = []
        lista_var_mensual = []
        lista_var_interanual = []
        lista_var_acumulada = []

        for indice, descripcion_categoria in zip(df_categorias['id_categoria'], df_categorias['categoria_descripcion']):
            serie = df_bdd[df_bdd['sector_productivo'] == indice].sort_values('fecha')

            if len(serie) < 13:
                print(f"⚠️ Sector {indice} ({descripcion_categoria}) no tiene 13 meses. Se salta.")
                continue

            valor_actual = serie['valor'].iloc[-1]
            valor_mes_anterior = serie['valor'].iloc[-2]
            valor_ano_anterior = serie['valor'].iloc[-13]

            # Buscar diciembre del año anterior
            anio_actual = fecha_maxima.year
            filtro_dic = (serie['fecha'].dt.year == anio_actual - 1) & (serie['fecha'].dt.month == 12)
            diciembre_anterior = serie[filtro_dic]

            if not diciembre_anterior.empty and diciembre_anterior['valor'].iloc[0] != 0:
                valor_diciembre_anterior = diciembre_anterior['valor'].iloc[0]
            else:
                valor_diciembre_anterior = np.nan

            # Calcular variaciones
            var_mensual = ((valor_actual / valor_mes_anterior) - 1) * 100 if valor_mes_anterior != 0 else 0
            var_intearnual = ((valor_actual / valor_ano_anterior) - 1) * 100 if valor_ano_anterior != 0 else 0
            var_acumulada = ((valor_actual / valor_diciembre_anterior) - 1) * 100 if pd.notna(valor_diciembre_anterior) else 0

            lista_indices.append(descripcion_categoria)
            lista_var_mensual.append(var_mensual)
            lista_var_interanual.append(var_intearnual)
            lista_var_acumulada.append(var_acumulada)

        # Construcción final del DataFrame
        data = {
            'nombre_indices': lista_indices,
            'var_mensual': lista_var_mensual,
            'var_interanual': lista_var_interanual,
            'var_acumulada': lista_var_acumulada
        }

        df = pd.DataFrame(data)

        df_var_mensual = df.sort_values(by='var_mensual', ascending=False).reset_index(drop=True)
        df_var_interanual = df.sort_values(by='var_interanual', ascending=False).reset_index(drop=True)
        df_var_acumulada = df.sort_values(by='var_acumulada', ascending=False).reset_index(drop=True)

        df_resultado = pd.DataFrame({
            'categoria_mensual': df_var_mensual['nombre_indices'],
            'var_mensual': df_var_mensual['var_mensual'].apply(self.truncar_decimal),
            'categoria_interanual': df_var_interanual['nombre_indices'],
            'var_interanual': df_var_interanual['var_interanual'].apply(self.truncar_decimal),
            'categoria_acumulada': df_var_acumulada['nombre_indices'],
            'var_acumulada': df_var_acumulada['var_acumulada'].apply(self.truncar_decimal)
        })

        return df_resultado, fecha_maxima


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
