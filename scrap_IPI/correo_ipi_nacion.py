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
        email_receptores = ['matizalazar2001@gmail.com']
        table_name = 'ipi'
        query_consulta = f'SELECT * FROM {table_name} ORDER BY fecha DESC LIMIT 1'
        df_bdd = pd.read_sql(query_consulta, self.conn)
        fecha_cadena = self.obtener_ultimafecha_actual(df_bdd["fecha"].iloc[-1])
        df_bdd = df_bdd.drop("fecha", axis=1)
        df_bdd = df_bdd.multiply(100)

        asunto = f'ACTUALIZACION - IPI - {fecha_cadena}'

        mensaje = f'''
        <html>
            <head>
                <style>
                    h2 {{
                        font-size: 24px;
                        color: #8b4513;
                        text-align: center;
                    }}
                    .container {{
                        width: 100%;
                    }}
                    .container-data-general {{
                        width: 100%;
                        text-align: center;
                        margin-bottom: 20px;
                    }}
                    .data-item-valor-general {{
                        display: flex;
                        justify-content: center;
                        align-items: center;
                        flex-direction: column;
                        border: 2px solid #8b4513;
                        border-radius: 10px;
                        padding: 10px;
                        background-color: #f1d0d0;
                        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
                        box-sizing: border-box;
                        width: 100%;
                        text-align: center; /* Asegurar centrado del texto */
                        margin: 0 auto;
                    }}
                    .container-data-general span {{
                        display: block;
                        width: 100%;
                        font-size: 24px;
                        color: #8b4513;
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
                        border: 2px solid #8b4513;
                        border-radius: 10px;
                        padding: 10px;
                        text-align: center;
                        background-color: #f1d0d0;
                        box-shadow: 2px 2px 5px rgba(0,0,0,0.1);
                        box-sizing: border-box;
                        margin: 10px;
                        width: 100%;
                    }}
                    .data-item span {{
                        font-weight: bold;
                    }}
                    .footer {{
                        font-size: 15px;
                        color: #888;
                        margin-top: 20px;
                        text-align: center;
                    }}
                    @media (max-width: 768px) {{
                        .data-item {{
                            width: 100%;
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
                    <h2><strong>INDICE DE PRODUCCION INDUSTRIAL MANUFACTURERO (IPI) A {fecha_cadena.upper()}.</strong></h2>
                    <div class='container-data-general'>
                        <div class="data-item-valor-general">
                        <img src="cid:ipi" alt="IPI Image" style="max-width: 70px; height: 70px; margin-bottom: 10px;">
                        <span>Variacion Interanual IPI: <strong>{df_bdd["var_IPI"].iloc[-1]:,.2f}%</strong></span>
                        </div>
                    </div>
                    <div class='container-data'>    
                        <div class="data-item-variaciones">
                        <br><img src="cid:alimentos" alt="ALimentos Image" style="max-width: 70px; height: 70px; margin-bottom: 10px;">
                        <br>
                        Variacion Interanual Alimentos
                        <br> <span>{df_bdd["var_interanual_alimentos"].iloc[-1]:,.2f}%</span>
                        </div>

                        <div class="data-item-variaciones">
                        <br><img src="cid:sueter" alt="Textil Image" style="max-width: 70px; height: 70px; margin-bottom: 10px;">
                        <br>
                        Variacion Interanual Textil 
                        <br><span>{df_bdd["var_interanual_textil"].iloc[-1]:,.2f}%</span>
                        </div>

                        <div class="data-item-variaciones">
                        <br><img src="cid:sustancia" alt="Sustancia Image" style="max-width: 70px; height: 70px; margin-bottom: 10px;">
                        <br>
                        Variacion Interanual Sustancias 
                        <br><span>{df_bdd["var_interanual_sustancias"].iloc[-1]:,.2f}%</span>
                        </div>

                        <div class="data-item-variaciones">
                        <br><img src="cid:maderas" alt="Maderas Image" style="max-width: 70px; height: 70px; margin-bottom: 10px;">
                        <br>
                        Variacion Interanual Maderas 
                        <br><span>{df_bdd["var_interanual_maderas"].iloc[-1]:,.2f}%</span>
                        </div>

                        <div class="data-item-variaciones">
                        <br><img src="cid:minerales_no_metalicos" alt="Min no metalicos Image" style="max-width: 70px; height: 70px; margin-bottom: 10px;">
                        <br>
                        Variacion Interanual min. No Metalicos 
                        <br><span>{df_bdd["var_interanual_MinNoMetalicos"].iloc[-1]:,.2f}%</span>
                        </div>

                        <div class="data-item-variaciones">
                        <br><img src="cid:metales" alt="Metales Image" style="max-width: 70px; height: 70px; margin-bottom: 10px;">
                        <br>
                        Variacion Interanual Metales 
                        <br><span>{df_bdd["var_interanual_metales"].iloc[-1]:,.2f}%</span>
                        </div>

                    </div>
                    <div class="footer">
                        Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes<br>
                        Dirección: Tucumán 1164 - Corrientes Capital<br>
                        Contacto Coordinación General: 3794 284993
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

        # Ruta de las imágenes
        image_paths = {
            "ipi": "/home/usuario/Escritorio/scrapingTrabajo/scrap_IPI/files/ipi.png",
            "sueter": "/home/usuario/Escritorio/scrapingTrabajo/scrap_IPI/files/sueter.png",
            "alimentos": "/home/usuario/Escritorio/scrapingTrabajo/scrap_IPI/files/alimentos.png",
            "sustancia": "/home/usuario/Escritorio/scrapingTrabajo/scrap_IPI/files/sustancias.png",
            "maderas": "/home/usuario/Escritorio/scrapingTrabajo/scrap_IPI/files/maderas.png",
            "minerales_no_metalicos": "/home/usuario/Escritorio/scrapingTrabajo/scrap_IPI/files/minerales_no_metalicos.png",
            "metales": "/home/usuario/Escritorio/scrapingTrabajo/scrap_IPI/files/metales.png"
        }

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

