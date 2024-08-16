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
            <body>
                <h2 style="font-size: 24px; color: #444; text-align: center;"><strong>DATOS NUEVOS DEL ESTIMADOR MENSUAL DE ACTIVIDAD ECONÓMICA (EMAE) A {cadena_fecha_actual.upper()}</strong></h2>
                <h3 style="font-size: 15px; color: #666; font-weight: 100; text-align: center;">EMAE es un indicador clave para medir la evolución de la actividad económica de Argentina en el corto plazo. Elaborado por INDEC, 
                    este ofrece una estimación mensual de la producción de bienes y servicios, reflejando la dinámica de los diferentes sectores económicos del país. 
                    Este índice es fundamental para el análisis económico y la toma de decisiones tanto en el ámbito público como privado.</h3>  
                    
                <div class="container-variaciones" style="justify-content: center; width: 100%; display: flex; flex-wrap: wrap; gap: 10px;">
                    <div class="box" style="justify-content: center; background-color: #106490; border-radius: 10px; padding: 15px; text-align: center; padding:10px; margin:10px;flex: 1 1 300px; max-width: 300px;">
                        <h4 style="font-size: 20px; color: white; margin-bottom: 10px;">Variación Mensual Desestacionalizada</h4>
                        <p style="font-size: 24px; color: white;">{var_mensual:.1f}%</p>
                    </div>
                    <div class="box" style="justify-content: center; background-color: #106490; border-radius: 10px; padding: 15px; text-align: center; padding:10px; margin:10px;flex: 1 1 300px; max-width: 300px;">
                        <h4 style="font-size: 20px; color: white; margin-bottom: 10px;">Variación Interanual</h4>
                        <p style="font-size: 24px; color: white;">{var_interanual:.1f}%</p>
                    </div>
                </div>
        <hr>
        '''
        
        #HEAD de la tabla, contiene los titulos
        cabeza_tabla_variaciones = f'''
        <h3> Variaciones a nivel Nacional del Estimador Mensual de Actividad Económico (EMAE) - Argentina </h3>
        <table style="border-collapse: collapse; width: 100%;">
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;">  VAR. MENSUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VAR. INTERANUAL </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> INDICE </th>
        <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VAR. ACUMULADA</th>
        '''
    
        #Ordenacion por var mensual | intearanual| acumulado
        df_mensual = df_variaciones.sort_values(by='var_mensual',ascending=[False])
        df_interanual = df_variaciones.sort_values(by='var_interanual',ascending=[False])
        df_acumulado = df_variaciones.sort_values(by='var_acumulada',ascending=[False])

        # Datos correspondientes a cada variacion
        for nombre_mensual, var_mensual, nombre_interanual, var_interanual, nombre_acumulado, var_acumulada in zip(df_mensual['nombre_indices'], df_mensual['var_mensual'], df_interanual['nombre_indices'], df_interanual['var_interanual'], df_acumulado['nombre_indices'], df_acumulado['var_acumulada']):
            fila_de_nea = f'''
                <tr>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {nombre_mensual}</td>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {var_mensual:.2f}%</td>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {nombre_interanual}</td>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {var_interanual:.2f}%</td>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {nombre_acumulado}</td>
                <td style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> {var_acumulada:.2f}%</td>
                </tr>
                '''
            cabeza_tabla_variaciones = cabeza_tabla_variaciones + fila_de_nea



        fin_mensaje = f'''
                </table> 
                <hr>
                <div class="footer" style="font-size: 15px; color: #888; text-align: center" >
                    <img src="cid:ipecd" alt="logo" style="margin-right: 20px; max-width: 250px; height: auto; pointer-events: none; user-select: none;" >
                </div>
            </body>
        </html>
        '''

        mensaje_final = cadena_inicio + cabeza_tabla_variaciones + fin_mensaje



        # ==== ENVIO DE MENSAJE

        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        #email_receptores =  ['benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com']
        email_receptores =  [ 'matizalazar2001@gmail.com', 'manumarder@gmail.com']
        email_receptores_str = ', '.join(email_receptores)


        em = MIMEMultipart()
        em['From'] = email_emisor
        em['To'] = email_receptores
        em['Subject'] = asunto
        em.attach(MIMEText(mensaje_final, 'html'))
        # Obtener el directorio actual donde se encuentra el script
        script_dir = os.path.dirname(__file__)

        # Definir la carpeta donde se encuentran las imágenes
        image_dir = os.path.join(script_dir, 'files')

        # Diccionario de nombres de archivos de imágenes
        image_files = {
            "ipecd": "logo_ipecd.png", 
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
        
