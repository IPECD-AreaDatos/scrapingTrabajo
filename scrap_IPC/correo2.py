#Archivo destino a la construccion de informes que se envian por Gmail.
from email.message import EmailMessage
from smtplib import SMTP_SSL
import pandas as pd
import os
from sqlalchemy import create_engine, text
from datetime import datetime
import mysql.connector
import calendar
from ssl import create_default_context
from sqlalchemy.orm import sessionmaker
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
        self.engine = create_engine(f'mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}')

    def obtener_correos(self):
        conn = mysql.connector.connect(
                host = self.host,
                user = self.user,
                password = self.password,
                database = 'ipecd_economico'
            )

        cursor = conn.cursor()

        # Consulta para obtener los correos
        consulta = "SELECT email FROM correos WHERE prueba = 1"
        #consulta = "SELECT email FROM correos"
        
        cursor.execute(consulta)
        correos = cursor.fetchall()

        # Convertir tuplas a lista de strings
        email_receptores = [correo[0] for correo in correos]

        # Cerrar la conexión
        conn.close()

        return email_receptores
    
    # Objetivo: Juntar todos los elementos del correo y darle forma para enviarlo.
    def enviar_correo(self, fecha_maxima, var_mensual, var_acumulada, var_interanual, df_var_nea, df_var_region ):

        #Pasamos la ultima fecha un formato mas legible
        cadena_fecha_actual = self.obtener_fecha_actual(fecha_maxima)        

        #Asunto del correo
        asunto = f'Actualizacion de datos IPC - {cadena_fecha_actual}'

        cadena_inicio = f'''
        <html>
        <head>
            <style>
                @media (max-width: 600px) {{
                    
                    .box-mapa {{ 
                        flex-direction: column !important; /* Cambia a columnas en lugar de filas */
                        align-items: center; /* Alinea los elementos al inicio */
                    }}
                    .mapa {{ 
                        margin-bottom: 5px;
                    }}
                    .tabla-reg {{
                        width: 95% !important; /* Asegura que la tabla ocupe todo el ancho disponible */
                        
                    }}
                
                @media (max-width: 1380px) {{
                    
                    .contenedor-horizontal {{ 
                        display: flex;
                        flex-wrap: wrap;
                    }}
                    .box-mapa, .tabla-grande {{ 
                        flex: 1 1 100%; /* Ocupa todo el ancho de la pantalla en pequeñas resoluciones */
                        max-width: 100%; /* Asegura que no se desborden */
                        margin-bottom: 20px;
                    }}
                    .tabla-grande {{
                        flex: 2;                        
                    }}
                }}
            </style>
        </head>
            <body style="color-scheme: light;">
                <div class="container" style= "color-scheme: light;background-image: url('cid:fondo'); background-repeat: no-repeat; background-position: center center; background-size: cover;">

                <h2 class="titulo" style="margin: 10px;font-size: 24px; color: #172147; -webkit-text-fill-color: #444 !important;text-align: center;"><strong>DATOS NUEVOS DEL INDICE DE PRECIOS AL CONSUMIDOR (IPC) A {cadena_fecha_actual.upper()}</strong></h2>
                <h3 class="descripcion" style="padding: 10px; font-size: 15px; color: #172147; -webkit-text-fill-color: #666 !important;font-weight: 180; text-align: center;">El IPC mide la variación de precios de los bienes y servicios representativos del gasto de consumo de los hogares residentes 
                        en la zona seleccionada en comparación con los precios vigentes en el año base.</h3>  
                    
               <div class="container-variaciones" style="color-scheme: light;width: 100%; display: flex; justify-content: center;">
                    <div class="box" style="width: 100%; border: 2px solid #172147; border-radius: 5px;text-align: center; margin-left: 40px; margin-right: 40px; margin-top: 10px; margin-bottom:10px;background-position-x: -75px; background-position-y: -149px; background-size: 150% auto; background-image: url('cid:fondo_var');">
                        <h4 class="variaciones-imp" style="font-size: 17px; font-weight: 200; color: white;-webkit-text-fill-color: white !important; ">VARIACIÓN MENSUAL: <strong>{var_mensual:.2f}%</strong></h4>
                        <h4 class="variaciones-imp" style="font-size: 17px;font-weight: 200; color: white; -webkit-text-fill-color: white !important;">VARIACIÓN INTERANUAL: <strong>{var_interanual:.2f}%</strong></h4>
                        <h4 class="variaciones-imp" style="font-size: 17px;font-weight: 200; color: white; -webkit-text-fill-color: white !important;">VARIACIÓN ACUMULADA: <strong>{var_acumulada:.2f}%</strong></h4>
                    </div>
                </div>
                <!-- Contenedor general que agrupa el mapa y ambas tablas -->
                <div class="contenedor-horizontal">
                <!-- Box con imagen a la izquierda y tabla a la derecha -->
                <div class="box-mapa" style="display: flex; align-items: center; justify-content: space-between; margin: 20px; border-radius: 8px; box-shadow: 0 0 10px rgba(0,0,0,0.1);">
                    <!-- Imagen -->
                    <div class="mapa" style="flex: 1; padding: 10px; margin-right: 10px">
                        <img src="cid:mapa_regiones" alt="Imagen Box" class="img-responsive" style="width: auto; border-radius: 8px; height: 380px;">
                    </div>
                    <!-- Tabla -->
                    <div class="tabla-reg" style="flex: 2; padding: 30px; ">
                        <table style="border-collapse: separate; border-spacing: 0; width: 100%; background-color: #f9f9f9; border-radius: 8px; overflow: hidden; box-shadow: 0 0 10px rgba(0,0,0,0.1);">
                            <thead>
                                <tr style="background-color: #313A63; color: #fff;">
                                    <th style="padding: 10px; text-align: left; font-size: 16px;">Region</th>
                                    <th style="padding: 10px; text-align: left; font-size: 16px;">Variación Mensual</th>
                                    <th style="padding: 10px; text-align: left; font-size: 16px;">Variación Interanual</th>
                                    <th style="padding: 10px; text-align: left; font-size: 16px;">Variación Acumulada</th>
                                </tr>
                            </thead>
                            <tbody>
        '''
        
        # Añadir filas a la tabla para df_var_region
        for _, row in df_var_region.iterrows():
            cadena_inicio += f'''
                <tr style="background-color: #fff; border-bottom: 1px solid #ddd;">
                    <td style="padding: 10px; font-size: 14px;">{row['id_region']}</td>
                    <td style="padding: 10px; font-size: 14px; color: #333;">{row['var_mensual']}</td>
                    <td style="padding: 10px; font-size: 14px; color: #333;">{row['var_interanual']}</td>
                    <td style="padding: 10px; font-size: 14px; color: #333;">{row['var_acumulada']}</td>
                </tr>
            '''
        
        cadena_inicio += f'''
                            </tbody>
                        </table>
                    </div>
                </div>
                
                <!-- Tabla de variaciones NEA -->
                <div class="tabla-grande" style="text-align: center; margin: 20px 0;">
                    <h2 class="titulo" style="margin: 30px; font-size: 25px; color: #172147; -webkit-text-fill-color: #444 !important;text-align: center;"><strong>Datos del NEA por subdivision.</strong></h2>

                    <table style="border-collapse: separate; border-spacing: 0; padding: 10px, 25px;margin: 0 auto; width: 95%; background-color: #f9f9f9; border-radius: 8px; overflow: hidden; box-shadow: 0 0 10px rgba(0,0,0,0.1);">
                        <thead>
                            <tr style="background-color: #313A63; color: #fff;">
                                <th style="padding: 10px; text-align: center; font-size: 16px;">Subdivision</th>
                                <th style="padding: 10px; text-align: center; font-size: 16px;">Variación Mensual</th>
                                <th style="padding: 10px; text-align: center; font-size: 16px;">Variación Interanual</th>
                                <th style="padding: 10px; text-align: center; font-size: 16px;">Variación Acumulada</th>
                            </tr>
                        </thead>
                        <tbody>
        '''
        
        # Añadir filas a la tabla para df_var_nea
        for _, row in df_var_nea.iterrows():
            cadena_inicio += f'''
                <tr style="background-color: #fff; border-bottom: 1px solid #ddd;">
                    <td style="padding: 10px; font-size: 14px;">{row['id_categoria']}</td>
                    <td style="padding: 10px; font-size: 14px; color: #333;">{row['var_mensual']}</td>
                    <td style="padding: 10px; font-size: 14px; color: #333;">{row['var_interanual']}</td>
                    <td style="padding: 10px; font-size: 14px; color: #333;">{row['var_acumulada']}</td>
                </tr>
            '''

        fin_mensaje = f'''
                        </tbody>
                    </table>
                </div>
                </div>
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
        email_receptores = self.obtener_correos()
        print(email_receptores)
        print("correo manu")

        msg = MIMEMultipart('related')
        msg['From'] = email_emisor
        msg['To'] = ', '.join(email_receptores)  # Convertir lista de correos a una cadena
        msg['Subject'] = asunto
        # Parte del contenido HTML
        parte_html = MIMEText(mensaje_final, 'html')
        msg.attach(parte_html)        
        # Obtener el directorio actual donde se encuentra el script
        script_dir = os.path.dirname(__file__)

        # Definir la carpeta donde se encuentran las imágenes
        image_dir = os.path.join(script_dir, 'files')

        # Diccionario de nombres de archivos de imágenes
        image_files = {
            "ipecd": "logo_ipecd.png", 
            "fondo": "fondo_correo.png",
            "fondo_var": "fondo_var.png",
            "mapa_regiones": "mapa_regiones.png"
        }

        # Construir las rutas completas y crear un diccionario para las rutas de las imágenes
        image_paths = {cid: os.path.join(image_dir, filename) for cid, filename in image_files.items()}

        # Adjuntar las imágenes
        for cid, filename in image_paths.items():
            path = os.path.join(image_dir, filename)
            with open(path, 'rb') as img_file:
                img = MIMEImage(img_file.read())
                img.add_header('Content-ID', f'<{cid}>')
                msg.attach(img)

        contexto = create_default_context()
        with SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, msg.as_string())

    # Objetivo: Convertir a formato string una fecha
    def obtener_fecha_actual(self,fecha_ultimo_registro):

        # Verificar si la fecha es una cadena, y convertirla a objeto datetime si es necesario
        if isinstance(fecha_ultimo_registro, str):
            fecha_ultimo_registro = datetime.strptime(fecha_ultimo_registro, '%Y-%m-%d %H:%M:%S')

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
        query_consulta = f"SELECT * FROM {nombre_tabla} WHERE id_categoria = 1 AND fecha = '{fecha}' ORDER BY fecha"
        df_bdd = pd.read_sql(query_consulta,self.engine)
        df_bdd = df_bdd.drop(['fecha', 'id_categoria', 'id_division', 'id_subdivision', 'valor'], axis=1)

        # Multiplicar las columnas por 100
        df_bdd['var_mensual'] *= 100
        df_bdd['var_interanual'] *= 100
        df_bdd['var_acumulada'] *= 100

        # Convertir los valores a porcentaje
        df_bdd['var_mensual'] = df_bdd['var_mensual'].apply(lambda x: f"{x:.2f}%")
        df_bdd['var_interanual'] = df_bdd['var_interanual'].apply(lambda x: f"{x:.2f}%")
        df_bdd['var_acumulada'] = df_bdd['var_acumulada'].apply(lambda x: f"{x:.2f}%")

        # Obtener nombres de categorías
        nombres = pd.read_sql("SELECT id_region, descripcion_region FROM identificador_regiones", self.engine)
        diccionario_nombres = dict(zip(nombres['id_region'], nombres['descripcion_region']))

        # Aplicar el mapeo de nombres a los datos
        df_bdd['id_region'] = df_bdd['id_region'].map(diccionario_nombres)


        print("VARIACIONES POR REGION")
        print(df_bdd)
        
        df_bdd = df_bdd.sort_values(by='var_mensual', ascending=[False])

        return df_bdd
    
    # Objetivo: Leer de la base las variaciones mensuales, anuales y acumuladas de las 13 categorias de la region del nea en la ultima fecha para tranformarlo en un df
    def variaciones_nea(self, fecha):
        nombre_tabla = 'ipc_valores'
        lista_subdivisiones = [1,2,14,17,20,25,27,30,35,37,41,42,44]

        # Obtener datos filtrados
        query_consulta = f""" SELECT fecha, id_region, id_categoria, id_subdivision, var_mensual, var_interanual, var_acumulada 
        FROM {nombre_tabla} 
        WHERE fecha = '{fecha}' AND id_region = 5 AND id_subdivision IN ({','.join(map(str, lista_subdivisiones))})
        """
        df_bdd = pd.read_sql(query_consulta, self.engine)

        df_bdd = df_bdd.drop(['fecha', 'id_region', 'id_subdivision'], axis=1)

        # Multiplicar las columnas por 100
        df_bdd['var_mensual'] *= 100
        df_bdd['var_interanual'] *= 100
        df_bdd['var_acumulada'] *= 100

        # Convertir los valores a porcentaje
        df_bdd['var_mensual'] = df_bdd['var_mensual'].apply(lambda x: f"{x:.2f}%")
        df_bdd['var_interanual'] = df_bdd['var_interanual'].apply(lambda x: f"{x:.2f}%")
        df_bdd['var_acumulada'] = df_bdd['var_acumulada'].apply(lambda x: f"{x:.2f}%")


        # Obtener nombres de categorías
        nombres = pd.read_sql("SELECT id_categoria, nombre FROM ipc_categoria", self.engine)
        diccionario_nombres = dict(zip(nombres['id_categoria'], nombres['nombre']))

        # Aplicar el mapeo de nombres a los datos
        df_bdd['id_categoria'] = df_bdd['id_categoria'].map(diccionario_nombres)


        # Verificar los datos obtenidos
        print("VARIACIONES NEA")
        print(df_bdd)

        # Ordenar el DataFrame por la columna 'var_mensual' de mayor a menor
        df_bdd = df_bdd.sort_values(by='var_mensual', ascending=False)

        return df_bdd

    def main(self):

        fecha_max = pd.read_sql("SELECT fecha FROM ipc_valores ORDER BY fecha DESC LIMIT 1", self.engine)

        Fecha_max = fecha_max['fecha'].iloc[0] 

        variable_fecha_max_str = Fecha_max.strftime('%Y-%m-%d %H:%M:%S')

        # Obtenemos variaciones nacionales de la ultima fecha
        var_mensual, var_interanual, var_acumulada = self.variaciones_nacion()
        
        # Obtenemos df de variaciones por region de la ultima fecha
        df_var_region = self.variaciones_region(variable_fecha_max_str)

        # Obtenemos df de variaciones extendido del nea de la ultima fecha
        df_var_nea = self.variaciones_nea(variable_fecha_max_str)

        # Enviamos el correo
        self.enviar_correo(variable_fecha_max_str, var_mensual, var_acumulada, var_interanual, df_var_nea, df_var_region)

