import os
from pymysql import connect
import pandas as pd
import calendar
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import ssl
import smtplib

class Correo_ipi_nacion:

    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.cursor = None
        self.conn = None

    def connect_bdd(self):
        self.conn = connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        self.cursor = self.conn.cursor()

    def construccion_correo(self):
        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        email_receptores = ['matizalazar2001@gmail.com', 'gastongrillo2001@gmail.com']
        
        #Valores de IPI Numeros Indices
        query_consulta_valores = f'SELECT * FROM ipi_valores ORDER BY fecha DESC LIMIT 1'
        df_numeros_indices = pd.read_sql(query_consulta_valores, self.conn)
        df_numeros_indices = df_numeros_indices.drop("fecha", axis=1)
        
        #Valores de IPI Variaciones_Interanual
        query_consulta_variacion_interanual = f'SELECT * FROM ipi_variacion_interanual ORDER BY fecha DESC LIMIT 1'
        df_variacion_interanual = pd.read_sql(query_consulta_variacion_interanual, self.conn)
        fecha_cadena = self.obtener_ultimafecha_actual(df_variacion_interanual["fecha"].iloc[-1])
        df_variacion_interanual = df_variacion_interanual.drop("fecha", axis=1)
        df_variacion_interanual = df_variacion_interanual.multiply(100)

        #Valores de IPI Variacion Mensual y Variacion Interanual Acumulada
        query_consulta_variacion = f'SELECT * FROM ipi_var_interacum ORDER BY fecha DESC LIMIT 1'
        df_variacion = pd.read_sql(query_consulta_variacion, self.conn)
        df_variacion = df_variacion.drop("fecha", axis=1)
        
        asunto = f'ACTUALIZACION - IPI - {fecha_cadena}'

        mensaje = f'''
        <html>
            <head>
                <style>
                    h2 {{
                        font-size: 24px;
                        color: #465c49;
                        text-align: center;
                    }}
                    .container {{
                        width: 100%;
                        background-image: url("cid:fondo");
                        background-size: cover;
                        background-position: center;
                        overflow: hidden; /* Oculta las barras de desplazamiento */
                    }}
                    .container-data-general {{
                        width: 100%;
                        text-align: center;
                        margin-bottom: 20px;
                    }}
                    .data-item-valor-general {{
                        display: inline-flex;
                        justify-content: center;
                        align-items: center;
                        border: 2px solid #465c49;
                        border-radius: 10px;
                        padding: 10px;
                        background-image: url("cid:fondo_cuadros");
                        background-size: cover;
                        background-position: center;
                        overflow: hidden; /* Oculta las barras de desplazamiento */
                        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
                        box-sizing: border-box;
                        width: auto;
                        text-align: center;
                        margin: 0 auto;
                    }}
                    .data-item-valor-general img {{
                        margin-right: 10px;
                    }}
                    .container-data-general span {{
                        display: block;
                        width: 100%;
                        font-size: 24px;
                        color: #ffffff;
                        text-align: center;
                        padding: 20px;
                    }}
                    .container-data {{
                        width: 100%;
                        display: flex;
                        flex-direction: column;
                        gap: 20px;
                    }}
                    .data-item-variaciones {{
                        display: block;
                        border: 2px solid #465c49;
                        border-radius: 10px;
                        padding: 10px;
                        text-align: center;
                        background-image: url("cid:fondo_cuadros");
                        background-size: cover;
                        background-position: center;
                        overflow: hidden; /* Oculta las barras de desplazamiento */
                        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
                        box-sizing: border-box;
                        margin: 10px;
                        width: 100%;
                        color: #ffffff;
                    }}
                    .data-item-variaciones span {{
                        color: #ffffff;
                    }}
                    .footer {{
                        font-size: 15px;
                        color: #888;
                        margin-top: 20px;
                        text-align: center;
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        padding-left: 25%;
                        padding-bottom: 20px;
                    }}
                    .footer img {{
                        margin-right: 20px;
                        max-width: 100px; /* Ajusta el tamaño del logo */
                        height: auto;
                    }}
                    .footer-text {{
                        text-align: left;
                    }}
                    @media (max-width: 768px) {{
                        .data-item {{
                            width: 100%;
                        }}
                        .container-footer {{
                            flex-direction: column;
                        }}
                        .footer img {{
                            margin-bottom: 10px;
                            margin-right: 0;
                        }}
                        .footer-text {{
                            text-align: center;
                        }}
                    }}
                    @media (max-width: 480px) {{
                        .data-item-valor-general {{
                            width: 100%;
                        }}
                    }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h2><strong>INDICE DE PRODUCCION INDUSTRIAL MANUFACTURERO (IPI) A {fecha_cadena.upper()}</strong></h2>
                    <div class='container-data-general'>
                        <div class="data-item-valor-general">
                        <img src="cid:ipi" alt="IPI Image" style="max-width: 200px; height: 200px; margin-bottom: 10px;">
                        <span>Nivel General <br>
                        Indice: <strong>{df_numeros_indices["ipi_manufacturero"].iloc[-1]:,.0f}</strong>
                        <br>
                        Variacion Interanual: <strong>{df_variacion_interanual["var_IPI"].iloc[-1]:,.2f}%</strong>
                        <br>
                        Variacion Interanual Acumulada: <strong>{df_variacion["ipi_manufacturero_inter_acum"].iloc[-1]:,.2f}%</strong>
                        <br>
                        Variacion Mensual: <strong>{df_numeros_indices["var_mensual_ipi_manufacturero"].iloc[-1]:,.2f}%</strong>
                        </span>
                        </div>
                    </div>
                    <div class='container-data'>    
                        <div class="data-item-variaciones">
                        <br><img src="cid:alimentos" alt="Alimentos Image" style="max-width: 70px; height: 70px; margin-bottom: 10px;">
                        <br>
                        <strong>Alimentos y Bebidas</strong>
                        <hr style="border: 1px solid #465c49;">
                        <span>
                        Indice: <strong>{df_numeros_indices["alimentos"].iloc[-1]:,.1f}</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Interanual: <strong>{df_variacion_interanual["var_interanual_alimentos"].iloc[-1]:,.2f}%</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Interanual Acumulada: <strong>{df_variacion["alimentos_inter_acum"].iloc[-1]:,.2f}%</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Mensual: <strong>{df_numeros_indices["var_mensual_alimentos"].iloc[-1]:,.2f}%</strong>
                        </span>
                        </div>
                        
                        <div class="data-item-variaciones">
                        <br><img src="cid:sueter" alt="Textil Image" style="max-width: 70px; height: 70px; margin-bottom: 10px;">
                        <br>
                        <strong> Productos Textiles </strong>
                        <hr style="border: 1px solid #465c49;">
                        <span>
                        Indice: <strong>{df_numeros_indices["textil"].iloc[-1]:,.1f}</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Interanual: <strong>{df_variacion_interanual["var_interanual_textil"].iloc[-1]:,.2f}%</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Interanual Acumulada: <strong>{df_variacion["textil_inter_acum"].iloc[-1]:,.2f}%</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Mensual: <strong>{df_numeros_indices["var_mensual_textil"].iloc[-1]:,.2f}%</strong>
                        </span>
                        </div>
                        
                        <div class="data-item-variaciones">
                        <br><img src="cid:sustancia" alt="Sustancia Image" style="max-width: 70px; height: 70px; margin-bottom: 10px;">
                        <br>
                        <strong>Sustancias y Productos Quimicos</strong>
                        <hr style="border: 1px solid #465c49;">
                        <span>
                        Indice: <strong>{df_numeros_indices["sustancias"].iloc[-1]:,.1f}</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Interanual: <strong>{df_variacion_interanual["var_interanual_sustancias"].iloc[-1]:,.2f}%</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Interanual Acumulada: <strong>{df_variacion["sustancias_inter_acum"].iloc[-1]:,.2f}%</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Mensual: <strong>{df_numeros_indices["var_mensual_sustancias"].iloc[-1]:,.2f}%</strong>
                        </span>
                        </div>

                        <div class="data-item-variaciones">
                        <br><img src="cid:maderas" alt="Maderas Image" style="max-width: 90px; height: 70px; margin-bottom: 10px;">
                        <br>
                        <strong>Madera, Papel, Edicion e Impresion</strong>
                        <hr style="border: 1px solid #465c49;">
                        <span>
                        Indice: <strong>{df_numeros_indices["maderas"].iloc[-1]:,.1f}</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Interanual: <strong>{df_variacion_interanual["var_interanual_maderas"].iloc[-1]:,.2f}%</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Interanual Acumulada: <strong>{df_variacion["maderas_inter_acum"].iloc[-1]:,.2f}%</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Mensual: <strong>{df_numeros_indices["var_mensual_maderas"].iloc[-1]:,.2f}%</strong>
                        </span>
                        </div>

                        <div class="data-item-variaciones">
                        <br><img src="cid:minerales_no_metalicos" alt="Min no metalicos Image" style="max-width: 70px; height: 70px; margin-bottom: 10px;">
                        <br>
                        <strong>Productos Minerales No Metalicos</strong>
                        <hr style="border: 1px solid #465c49;">
                        <span>
                        Indice: <strong>{df_numeros_indices["min_no_metalicos"].iloc[-1]:,.1f}</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Interanual: <strong>{df_variacion_interanual["var_interanual_MinNoMetalicos"].iloc[-1]:,.2f}%</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Interanual Acumulada: <strong>{df_variacion["min_no_metalicos_inter_acum"].iloc[-1]:,.2f}%</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Mensual: <strong>{df_numeros_indices["var_mensual_min_no_metalicos"].iloc[-1]:,.2f}%</strong>
                        </span>
                        </div>

                        <div class="data-item-variaciones">
                        <br><img src="cid:metales" alt="Metales Image" style="max-width: 70px; height: 70px; margin-bottom: 10px;">
                        <br>
                        <strong>Productos de Metal</strong>
                        <hr style="border: 1px solid #465c49;">
                        <span>
                        Indice: <strong>{df_numeros_indices["metales"].iloc[-1]:,.1f}</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Interanual: <strong>{df_variacion_interanual["var_interanual_metales"].iloc[-1]:,.2f}%</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Interanual Acumulada: <strong>{df_variacion["metales_inter_acum"].iloc[-1]:,.2f}%</strong>
                        <hr style="border: 0px solid #f1d0d0;">
                        Variacion Mensual: <strong>{df_numeros_indices["var_mensual_min_metales"].iloc[-1]:,.2f}%</strong>
                        </span>
                        </div>

                    </div>
                    <div class="container-footer">
                        <div class="footer">
                            <img src="cid:ipecd" alt="IPI Image">
                            <div class="footer-text">
                                Instituto Provincial de Estadística y Ciencia de Datos de Corrientes<br>
                                Dirección: Tucumán 1164 - Corrientes Capital<br>
                                Contacto Coordinación General: 3794-284993
                            </div>
                        </div>
                    </div>
                </div>
            </body>
        </html>
        '''

        # Crear el mensaje de correo electrónico
        em = MIMEMultipart()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = asunto

        # Adjuntar el cuerpo del mensaje HTML
        em.attach(MIMEText(mensaje, 'html'))

        # Obtener el directorio actual donde se encuentra el script
        script_dir = os.path.dirname(__file__)

        # Definir la carpeta donde se encuentran las imágenes
        image_dir = os.path.join(script_dir, 'files')

        # Diccionario de nombres de archivos de imágenes
        image_files = {
            "fondo": "fondo_correo.jpg",
            "fondo_cuadros": "fondo_cuadros.jpg",
            "ipecd": "logo_ipecd.png",
            "ipi": "ipi.png",
            "sueter": "sueter.png",
            "alimentos": "alimentos.png",
            "sustancia": "sustancias.png",
            "maderas": "maderas.png",
            "minerales_no_metalicos": "minerales_no_metalicos.png",
            "metales": "metales.png"
        }

        # Construir las rutas completas y crear un diccionario para las rutas de las imágenes
        image_paths = {cid: os.path.join(image_dir, filename) for cid, filename in image_files.items()}

        # Adjuntar las imágenes
        for cid, path in image_paths.items():
            with open(path, 'rb') as img_file:
                img = MIMEImage(img_file.read())
                img.add_header('Content-ID', f'<{cid}>')
                em.attach(img)

        # Configurar el servidor SMTP y enviar el correo
        contexto = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())

    def obtener_ultimafecha_actual(self, fecha_ultimo_registro):
        nombre_mes_ingles = calendar.month_name[fecha_ultimo_registro.month]
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
        nombre_mes_espanol = traducciones_meses.get(nombre_mes_ingles, nombre_mes_ingles)
        return f"{nombre_mes_espanol} del {fecha_ultimo_registro.year}"

    def main(self):
        self.connect_bdd()
        self.construccion_correo()

