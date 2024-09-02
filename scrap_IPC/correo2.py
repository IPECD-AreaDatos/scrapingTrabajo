#Archivo destino a la construccion de informes que se envian por Gmail.
from email.message import EmailMessage
from smtplib import SMTP_SSL
import pandas as pd
import os
from sqlalchemy import create_engine
from datetime import datetime
import calendar
from ssl import create_default_context
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from dotenv import load_dotenv
from email.mime.image import MIMEImage

class InformeIPC:

    #Declaracion de atributos
    def __init__(self,host,user,password,database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

        #Creacion de conexion a la bdd
        self.engine = create_engine('mysql+pymysql://root:tu_contraseña@localhost:3306/mi_base_de_datos')

    # Objetivo: Juntar todos los elementos del correo y darle forma para enviarlo.
    def enviar_correo(self, fecha_maxima, var_mensual, var_acumulada, var_interanual, df_var_nea, df_var_region ):

        #Pasamos la ultima fecha un formato mas legible
        cadena_fecha_actual = self.obtener_fecha_actual(fecha_maxima)

        #Asunto del correo
        asunto = f'Actualizacion de datos IPC - {cadena_fecha_actual}'

        cadena_inicio = f'''
        <html>
            <body style="color-scheme: light;">
                <div class="container" style= "color-scheme: light;background-image: url('cid:fondo'); background-repeat: no-repeat; background-position: center center; background-size: cover;">

                <h2 class="titulo" style="font-size: 24px; color: #444; -webkit-text-fill-color: #444 !important;text-align: center;"><strong>DATOS NUEVOS DEL INDICE DE PRECIOS AL CONSUMIDOR (IPC) A {cadena_fecha_actual.upper()}</strong></h2>
                <h3 class="descripcion" style="font-size: 15px; color: #666; -webkit-text-fill-color: #666 !important;font-weight: 180; text-align: center;">El IPC mide la variación de precios de los bienes y servicios representativos del gasto de consumo de los hogares residentes 
                        en la zona seleccionada en comparación con los precios vigentes en el año base.</h3>  
                    
               <div class="container-variaciones" style="color-scheme: light;width: 100%; display: flex; justify-content: center;">
                    <div class="box" style="width: 100%; border: 2px solid #465c49; border-radius: 5px;text-align: center; margin-left: 40px; margin-right: 40px; margin-top: 10px; margin-bottom:10px;background-position-x: -75px; background-position-y: -149px; background-size: cover; background-image: url('cid:fondo_cuadros');">
                        <h4 class="variaciones-imp" style="font-size: 17px; font-weight: 200; color: white;-webkit-text-fill-color: white !important; ">VARIACIÓN MENSUAL: <strong>{var_mensual:.2f}%</strong></h4>
                        <h4 class="variaciones-imp" style="font-size: 17px;font-weight: 200; color: white; -webkit-text-fill-color: white !important;">VARIACIÓN INTERANUAL: <strong>{var_interanual:.2f}%</strong></h4>
                        <h4 class="variaciones-imp" style="font-size: 17px;font-weight: 200; color: white; -webkit-text-fill-color: white !important;">VARIACIÓN ACUMULADA: <strong>{var_acumulada:.2f}%</strong></h4>
                    </div>
                </div>
        '''

        fin_mensaje = f'''
                <br>
                <div class="footer" style="font-size: 15px; color: #888; text-align: center" >
                    <img src="cid:ipecd" alt="logo" style="margin-right: 20px; max-width: 250px; height: auto; pointer-events: none; user-select: none;" >
                </div>
                </div>
            </body>
        </html>
        '''

        # Juntamos todas las partes del correo que teniamos
        mensaje_final = cadena_inicio + fin_mensaje

        # ==== ENVIO DE MENSAJE

        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','manumarder@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com']
        email_receptores =  ['matizalazar2001@gmail.com','manumarder@gmail.com']
        email_receptores_str = ', '.join(email_receptores)

        em = MIMEMultipart()
        em['From'] = email_emisor
        em['To'] = email_receptores_str
        em['Subject'] = asunto
        em.attach(MIMEText(mensaje_final, 'html'))
        # Obtener el directorio actual donde se encuentra el script
        script_dir = os.path.dirname(__file__)

        # Definir la carpeta donde se encuentran las imágenes
        image_dir = os.path.join(script_dir, 'files')

        # Diccionario de nombres de archivos de imágenes
        image_files = {
            "ipecd": "logo_ipecd.png", 
            "fondo": "fondo_correo.png",
            "fondo_cuadros": "fondo_cuadros.png"
        }

        # Construir las rutas completas y crear un diccionario para las rutas de las imágenes
        image_paths = {cid: os.path.join(image_dir, filename) for cid, filename in image_files.items()}

        # Adjuntar las imágenes
        for cid, filename in image_paths.items():
            path = os.path.join(image_dir, filename)
            with open(path, 'rb') as img_file:
                img = MIMEImage(img_file.read())
                img.add_header('Content-ID', f'<{cid}>')
                em.attach(img)

        contexto = create_default_context()
        with SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())

    # Objetivo: Convertir a formato string una fecha
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
    
    #Objetivo: calcular la variacion mensual, intearanual y acumulado del IPC a nivel nacional
    def variaciones_nacion(self):

        nombre_tabla = 'ipc_valores'
        query_consulta = f'SELECT * FROM {nombre_tabla} WHERE id_region = 1 AND id_categoria = 1 ORDER BY fecha DESC LIMIT 1'
        #query_consulta = f'SELECT * FROM {nombre_tabla} WHERE id_region = 1 and id_categoria = 1'
        df_bdd = pd.read_sql(query_consulta,self.engine)

        print("VARIAICONES NACIONALES")
        print(df_bdd)

        # Asegúrate de que el DataFrame no esté vacío
        if not df_bdd.empty:
            variacion_mensual = df_bdd['var_mensual'].iloc[0] * 100
            variacion_interanual = df_bdd['var_interanual'].iloc[0] * 100
            variacion_acumulada = df_bdd['var_acumulada'].iloc[0] * 100
        else:
            # Manejo de caso cuando no hay datos en el DataFrame
            variacion_mensual = None
            variacion_interanual = None
            variacion_acumulada = None

        return variacion_mensual,variacion_interanual,variacion_acumulada
        
    # Objetivo: Leer de la base las variaicones por region en la ultima fecha para tranformarlo en un df
    def variaciones_region(self, fecha):
        nombre_tabla = 'ipc_valores'
        query_consulta = f'SELECT * FROM {nombre_tabla} WHERE fecha = {fecha} ORDER BY fecha DESC LIMIT 1'
        #query_consulta = f'SELECT * FROM {nombre_tabla} WHERE id_region = 1 and id_categoria = 1'
        df_bdd = pd.read_sql(query_consulta,self.engine)

        print("VARIACIONES POR REGION")
        print(df_bdd)
        
        df_bdd = df_bdd.sort_values(by='var_mensual', ascending=[False])

        return df_bdd
    
    # Objetivo: Leer de la base las variaciones desagregadas de la region del nea en la ultima fecha para tranformarlo en un df
    def variaciones_nea(self, fecha):
        nombre_tabla = 'ipc_valores'
        query_consulta = f'SELECT * FROM {nombre_tabla} WHERE fecha = {fecha} AND id_region = 5 ORDER BY fecha DESC LIMIT 1'
        #query_consulta = f'SELECT * FROM {nombre_tabla} WHERE id_region = 1 and id_categoria = 1'
        df_bdd = pd.read_sql(query_consulta,self.engine)

        print("VARIAICONES NEA")
        print(df_bdd)

        df_bdd = df_bdd.sort_values(by='var_mensual', ascending=[False])

        return df_bdd

    def main(self):

        variable_fecha_max = datetime.strptime('2024-07-01', '%Y-%m-%d')
        
        # Obtenemos variaciones nacionales de la ultima fecha
        var_mensual, var_interanual, var_acumulada = self.variaciones_nacion()
        
        # Obtenemos df de variaciones por region de la ultima fecha
        df_var_region = self.variaciones_region(variable_fecha_max)

        # Obtenemos df de variaciones extendido del nea de la ultima fecha
        df_var_nea = self.variaciones_nea(variable_fecha_max)

        # Enviamos el correo
        self.enviar_correo(variable_fecha_max, var_mensual, var_acumulada, var_interanual, df_var_nea, df_var_region)


