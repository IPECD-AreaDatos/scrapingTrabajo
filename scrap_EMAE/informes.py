#Archivo destino a la construccion de informes que se envian por Gmail.
import calendar
from email.message import EmailMessage
from smtplib import SMTP_SSL
import pandas as pd
from ssl import create_default_context
from pymysql import connect
import os
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage


class InformesEmae:

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

    
    #Objetivo: construir el correo que se enviara por GMAIL de forma interna
    def enviar_correo(self,df_variaciones,fecha_maxima):

        #Pasamos la ultima fecha un formato mas legible
        cadena_fecha_actual = self.obtener_fecha_actual(fecha_maxima)

        #Asunto del correo
        asunto = f'Actualizacion de datos EMAE - {cadena_fecha_actual}'

        var_mensual, var_interanual = self.obtener_variacion_anualymensual()


        cadena_inicio = f'''
        <html>
        <head>
            <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css" rel="stylesheet">
        </head>
            <body >
                <div class="container" style= "background-color: #ffffff; background-image: url('cid:fondo'); background-repeat: no-repeat; background-position: center center; background-size: cover;">

                <h2 style="font-size: 24px; color: #444; text-align: center;"><strong>DATOS NUEVOS DEL ESTIMADOR MENSUAL DE ACTIVIDAD ECONÓMICA (EMAE) A {cadena_fecha_actual.upper()}</strong></h2>
                <h3 style="font-size: 15px; color: #666; font-weight: 100; text-align: center;">EMAE es un indicador clave para medir la evolución de la actividad económica de Argentina en el corto plazo. Elaborado por INDEC, 
                    este ofrece una estimación mensual de la producción de bienes y servicios, reflejando la dinámica de los diferentes sectores económicos del país. 
                    Este índice es fundamental para el análisis económico y la toma de decisiones tanto en el ámbito público como privado.</h3>  
                    
               <div class="container-variaciones" style="width: 100%; display: flex; justify-content: center;">
                    <div class="box" style="width: 100%; border: 2px solid #465c49; border-radius: 5px;text-align: center; margin-left: 40px; margin-right: 40px; margin-top: 10px; margin-bottom:10px;background-position-x: -75px; background-position-y: -149px; background-size: cover; background-image: url('cid:fondo_cuadros');">
                        <h4 style="font-size: 17px; font-weight: 200; color: white; ">VARIACIÓN MENSUAL DESESTACIONALIZADA: {var_mensual:.1f}%</h4>
                        <h4 style="font-size: 17px;font-weight: 200; color: white; ">VARIACIÓN INTERANUAL: {var_interanual:.1f}%</h4>
                    </div>
                </div>
       
        '''
        html_content = '''
        
        <div class="container">

            <br>
            <h3 style="justify-content: center; text-align: center; margin: 0 auto;"> Variaciones a nivel Nacional de EMAE - Argentina </h3>
            <br>
            <div class="row" style="display: flex;flex-wrap: wrap;justify-content: center;">
        '''

                
        # Añadir boxes al HTML
        for index, row in df_variaciones.iterrows():
            html_content += f'''
            <div class="col-md-4 d-flex justify-content-center mb-4">
                <div class="circle-box" style="background-color: #106490;border-radius: 50%;padding: 20px;text-align: center;color: white;display: flex;flex-direction: column;justify-content: center;align-items: center;height: 150px;width: 150px;margin: 10px auto;box-shadow: 0 4px 8px rgba(0,0,0,0.2);">
                    <h4 style="font-size: 17px; font-weight: 200; margin-bottom: 10px;">{row['nombre_indices']}</h4>
                    <p style="font-size: 16px; margin: 0;">Var. Mensual: {row['var_mensual']:.1f}%</p>
                    <p style="font-size: 16px; margin: 0;">Var. Interanual: {row['var_interanual']:.1f}%</p>
                    <p style="font-size: 16px; margin: 0;">Var. Acumulada: {row['var_acumulada']:.1f}%</p>
                </div>
            </div>
        '''

        # Cerrar el HTML
        html_content += '''
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

        mensaje_final = cadena_inicio + html_content + fin_mensaje






        # ==== ENVIO DE MENSAJE

        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com']
        email_receptores =  [ 'matizalazar2001@gmail.com', 'manumarder@gmail.com']
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

        df = pd.DataFrame(data)

        return df, fecha_maxima


        
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
        var_mensual = df_bdd['variacion_mensual'].values[0]
        var_interanual = df_bdd['variacion_interanual'].values[0]
        
        return var_mensual, var_interanual


    #Enviamos los mensajes a cada plataforma
    def main_correo(self):
        
        #Conexion a la bdd
        self.conectar_bdd()

        #Obtencion de datos
        df_variaciones,fecha_maxima = self.variaciones_mensual_interanual_acumulada()

        #Envio de mensajes
        self.enviar_correo(df_variaciones,fecha_maxima)
        
