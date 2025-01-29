import os
import pandas as pd
import smtplib
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import locale
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

class SendMail:
    def __init__(self, host, user, password, database):
        locale.setlocale(locale.LC_TIME, 'es_ES.UTF-8')
        self.database = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        # Obtener la ruta de la carpeta de guardado
        self.carpeta_guardado = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'files')

    def extract_data(self):
        # Obtener la fecha actual y la fecha de un mes atrás
        fecha_actual = datetime.now()
        fecha_mes_atras = fecha_actual - timedelta(days=30)
        
        # Formatear las fechas en el formato adecuado para SQL
        fecha_actual_str = fecha_actual.strftime('%Y-%m-%d')
        fecha_mes_atras_str = fecha_mes_atras.strftime('%Y-%m-%d')
        
        # Consultas SQL para extraer los datos del último mes
        query_oficial = f"SELECT * FROM dolar_oficial WHERE fecha BETWEEN '{fecha_mes_atras_str}' AND '{fecha_actual_str}'"
        query_blue = f"SELECT * FROM dolar_blue WHERE fecha BETWEEN '{fecha_mes_atras_str}' AND '{fecha_actual_str}'"
        query_mep = f"SELECT * FROM dolar_mep WHERE fecha BETWEEN '{fecha_mes_atras_str}' AND '{fecha_actual_str}'"
        query_ccl = f"SELECT * FROM dolar_ccl WHERE fecha BETWEEN '{fecha_mes_atras_str}' AND '{fecha_actual_str}'"
        
        # Extraer los datos
        df_oficial = pd.read_sql(query_oficial, con=self.database)
        df_blue = pd.read_sql(query_blue, con=self.database)
        df_mep = pd.read_sql(query_mep, con=self.database)
        df_ccl = pd.read_sql(query_ccl, con=self.database)
        
        return df_oficial, df_blue, df_mep, df_ccl

    def generate_graph(self, df, title, filename, y_label):
        plt.style.use('ggplot')
        fig, ax = plt.subplots(figsize=(12, 6))
        
        if 'compra' in df.columns and 'venta' in df.columns:
            ax.plot(df['fecha'], df['compra'], marker='o', label='Compra', color='blue')
            ax.plot(df['fecha'], df['venta'], marker='o', label='Venta', color='orange')
            for i, txt in enumerate(df['compra']):
                ax.annotate(f"{txt:.2f}", (df['fecha'].iat[i], df['compra'].iat[i]), textcoords="offset points", xytext=(0, 5), ha='center', fontsize=10, color='blue')
            for i, txt in enumerate(df['venta']):
                ax.annotate(f"{txt:.2f}", (df['fecha'].iat[i], df['venta'].iat[i]), textcoords="offset points", xytext=(0, 5), ha='center', fontsize=10, color='orange')
        else:
            ax.plot(df['fecha'], df['valor'], marker='o', label='Valor', color='green')
            for i, txt in enumerate(df['valor']):
                ax.annotate(f"{txt:.2f}", (df['fecha'].iat[i], df['valor'].iat[i]), textcoords="offset points", xytext=(0, 5), ha='center', fontsize=10, color='green')
        
        ax.set_xlabel('Fecha', fontsize=14)
        ax.set_ylabel(y_label, fontsize=14)
        ax.set_title(title, fontsize=16, fontweight='bold')
        ax.legend(fontsize=12)
        ax.grid(True, which='both', linestyle='--', linewidth=0.5)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(self.carpeta_guardado, filename), dpi=300)
        plt.close()

    def calcular_variacion(self, df, columna):
        df = df.sort_values(by='fecha')
        df['variacion'] = df[columna].pct_change() * 100
        return df

    def format_variacion(self, valor):
        if valor > 0:
            return f"🔺 +{valor:.2f}%"
        elif valor < 0:
            return f"🔻 {valor:.2f}%"
        else:
            return f"➖ {valor:.2f}%"

    def send_mail(self, df_oficial, df_blue, df_mep, df_ccl):
        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        #email_receptores =  ['samaniego18@gmail.com','manumarder@gmail.com','benitezeliogaston@gmail.com', 'matizalazar2001@gmail.com','rigonattofranco1@gmail.com','boscojfrancisco@gmail.com','joseignaciobaibiene@gmail.com','ivanfedericorodriguez@gmail.com','agusssalinas3@gmail.com', 'rociobertonem@gmail.com','lic.leandrogarcia@gmail.com','pintosdana1@gmail.com', 'paulasalvay@gmail.com', 'samaniego18@gmail.com', 'guillermobenasulin@gmail.com', 'leclerc.mauricio@gmail.com']
        email_receptores =  [ 'matizalazar2001@gmail.com', 'manumarder@gmail.com']

        # Calcular la variación respecto al día anterior
        df_oficial = self.calcular_variacion(df_oficial, 'compra')
        df_blue = self.calcular_variacion(df_blue, 'compra')
        df_mep = self.calcular_variacion(df_mep, 'valor')
        df_ccl = self.calcular_variacion(df_ccl, 'valor')

        # Generar gráficos
        self.generate_graph(df_oficial, 'Evolución Dólar Oficial', 'dolar_oficial.png', 'Valor')
        self.generate_graph(df_blue, 'Evolución Dólar Blue', 'dolar_blue.png', 'Valor')
        self.generate_graph(df_mep, 'Evolución Dólar MEP', 'dolar_mep.png', 'Valor')
        self.generate_graph(df_ccl, 'Evolución Dólar CCL', 'dolar_ccl.png', 'Valor')

        # Obtener el último valor disponible y su variación
        ultimo_oficial = df_oficial.iloc[-1]
        ultimo_blue = df_blue.iloc[-1]
        ultimo_mep = df_mep.iloc[-1]
        ultimo_ccl = df_ccl.iloc[-1]

        # Obtener la fecha actual en el formato deseado en español con la primera letra del mes en mayúscula
        fecha_actual = datetime.now().strftime("%d de %B").title()

        # Crear un objeto MIMEMultipart
        msg = MIMEMultipart()
        msg['Subject'] = f'Cotizaciones Dolares - {fecha_actual}'
        msg['From'] = email_emisor
        msg['To'] = ', '.join(email_receptores)

        # Crear el cuerpo del correo electrónico con mejor formato sin tablas
        body = f"""\
        <html>
            <head>
                <style>
                    .cotizacion-container {{
                        display: flex;
                        flex-wrap: wrap;
                        gap: 20px;
                        margin-top: 20px;
                        justify-content: center;
                    }}
                    .cotizacion-item {{
                        flex: 1 1 calc(50% - 40px);
                        border: 1px solid #ccc;
                        padding: 10px;
                        border-radius: 8px;
                        box-shadow: 2px 2px 12px rgba(0,0,0,0.1);
                        text-align: center;
                        margin: 10px;
                    }}
                    .dolar-oficial {{
                        border-color: green;
                        color: green;
                    }}
                    .dolar-blue {{
                        border-color: blue;
                        color: blue;
                    }}
                    .cotizacion-header {{
                        font-size: 18px;
                        font-weight: bold;
                        margin-bottom: 5px;
                    }}
                    .cotizacion-variacion {{
                        font-size: 16px;
                        margin-bottom: 10px;
                    }}
                    .cotizacion-value {{
                        font-size: 16px;
                    }}
                </style>
            </head>
            <body>
                <h2>Las cotizaciones del dia del dólar oficial, blue, mep y ccl son las siguientes:</h2>
                <div class="cotizacion-container">
                    <div class="cotizacion-item dolar-oficial">
                        <div class="cotizacion-header">🏦Dólar Oficial</div>
                        <div class="cotizacion-variacion">{self.format_variacion(ultimo_oficial['variacion'])}</div>
                        <div class="cotizacion-value">Compra: ${ultimo_oficial['compra']:.2f}</div>
                        <div class="cotizacion-value">Venta: ${ultimo_oficial['venta']:.2f}</div>
                    </div>
                    <div class="cotizacion-item dolar-blue">
                        <div class="cotizacion-header">💵Dólar Blue</div>
                        <div class="cotizacion-variacion">{self.format_variacion(ultimo_blue['variacion'])}</div>
                        <div class="cotizacion-value">Compra: ${ultimo_blue['compra']:.2f}</div>
                        <div class="cotizacion-value">Venta: ${ultimo_blue['venta']:.2f}</div>
                    </div>
                    <div class="cotizacion-item">
                        <div class="cotizacion-header">💰Dólar MEP</div>
                        <div class="cotizacion-variacion">{self.format_variacion(ultimo_mep['variacion'])}</div>
                        <div class="cotizacion-value">Valor: ${ultimo_mep['valor']:.2f}</div>
                    </div>
                    <div class="cotizacion-item">
                        <div class="cotizacion-header">💰Dólar CCL</div>
                        <div class="cotizacion-variacion">{self.format_variacion(ultimo_ccl['variacion'])}</div>
                        <div class="cotizacion-value">Valor: ${ultimo_ccl['valor']:.2f}</div>
                    </div>
                </div>
                <p>
                    Instituto Provincial de Estadística y Ciencia de Datos de Corrientes<br>
                    Dirección: Tucumán 1164 - Corrientes Capital<br>
                    Contacto Coordinación General: 3794 284993
                </p>
            </body>
        </html>
        """
        
        # Adjuntar el cuerpo del correo
        msg.attach(MIMEText(body, 'html'))

        # Adjuntar gráficos
        for filename in ['dolar_oficial.png', 'dolar_blue.png', 'dolar_mep.png', 'dolar_ccl.png']:
            with open(os.path.join(self.carpeta_guardado, filename), 'rb') as f:
                file_data = f.read()
                attachment = MIMEText(file_data, 'base64', 'utf-8')
                attachment.add_header('Content-Disposition', 'attachment', filename=filename)
                attachment.add_header('Content-Transfer-Encoding', 'base64')
                msg.attach(attachment)

        # Enviar el correo electrónico usando un contexto de administrador con SSL
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.send_message(msg)