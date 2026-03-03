"""
MAIL - Módulo de envío de informes EMAE
Responsabilidad: Calcular variaciones y enviar reporte por Gmail (PostgreSQL compatible)
"""
import os
import logging
import calendar
import smtplib
import ssl
import pandas as pd
import numpy as np
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

class EmailEMAE:
    def __init__(self, host, user, password, database, port=None, version="1"):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.version = version
        self.engine = None
        self._init_engine()

        # Database for emails (often different in this project)
        self.database_emails = os.getenv('NAME_IPECD_ECONOMICO', 'ipecd_economico')

    def _init_engine(self):
        if self.version == "1":  # MySQL
            puerto = int(self.port) if self.port else 3306
            url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
        else:  # PostgreSQL
            puerto = int(self.port) if self.port else 5432
            url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database}"
        self.engine = create_engine(url)

    def _get_engine_emails(self):
        # Email database typically uses the same host/user but different DB name
        if self.version == "1":
            puerto = int(self.port) if self.port else 3306
            url = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{puerto}/{self.database_emails}"
        else:
            puerto = int(self.port) if self.port else 5432
            url = f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{puerto}/{self.database_emails}"
        return create_engine(url)

    def main_correo(self):
        logger.info("[MAIL] Iniciando proceso de envío de correo...")
        try:
            # 1. Obtener variaciones por categoría
            df_variaciones, fecha_maxima = self._calcular_variaciones()
            if df_variaciones.empty:
                logger.warning("[MAIL] No hay datos para el correo.")
                return

            # 2. Obtener variaciones generales
            var_mensual, var_interanual = self._obtener_variaciones_generales()

            # 3. Preparar HTML
            fecha_texto = self._formatear_fecha(fecha_maxima)
            html_content = self._render_template(var_mensual, var_interanual, df_variaciones)

            # 4. Enviar
            self._enviar(html_content, fecha_texto)
            logger.info("[MAIL] Correo enviado correctamente.")
        except Exception as e:
            logger.error("[MAIL] Error enviando correo: %s", e, exc_info=True)

    def _calcular_variaciones(self):
        # En Postgres las columnas suelen ser 'id_sector'
        col_sector = 'id_sector' if self.version == '2' else 'sector_productivo'
        
        df_bdd = pd.read_sql("SELECT * FROM emae", self.engine)
        # Nota: La tabla de categorías podría no estar en el mismo DB o tener nombre diferente.
        # Basado en el código original, se asume que existe 'emae_categoria'
        try:
            df_cats = pd.read_sql("SELECT * FROM emae_categoria", self.engine)
        except:
            logger.error("[MAIL] No se encontró tabla emae_categoria")
            return pd.DataFrame(), None

        if df_bdd.empty:
            return pd.DataFrame(), None

        df_bdd['fecha'] = pd.to_datetime(df_bdd['fecha'])
        fecha_max = df_bdd['fecha'].max()
        
        results = []
        for _, cat in df_cats.iterrows():
            idx = cat['id_categoria']
            desc = cat['categoria_descripcion']
            
            serie = df_bdd[df_bdd[col_sector] == idx].sort_values('fecha')
            if len(serie) < 13: continue

            val_act = serie['valor'].iloc[-1]
            val_ant = serie['valor'].iloc[-2]
            val_ia  = serie['valor'].iloc[-13]

            # Diciembre año pasado para acumulada
            diciembre_ant = serie[(serie['fecha'].dt.year == fecha_max.year - 1) & (serie['fecha'].dt.month == 12)]
            val_dic = diciembre_ant['valor'].iloc[0] if not diciembre_ant.empty else np.nan

            results.append({
                'nombre_indices': desc,
                'var_mensual': ((val_act / val_ant) - 1) * 100 if val_ant != 0 else 0,
                'var_interanual': ((val_act / val_ia) - 1) * 100 if val_ia != 0 else 0,
                'var_acumulada': ((val_act / val_dic) - 1) * 100 if pd.notna(val_dic) and val_dic != 0 else 0
            })

        df = pd.DataFrame(results)
        if df.empty: return df, fecha_max

        # Formatear para el template (truncar y reordenar como el original)
        df['var_mensual'] = df['var_mensual'].apply(lambda x: np.floor(x * 100) / 100)
        df['var_interanual'] = df['var_interanual'].apply(lambda x: np.floor(x * 100) / 100)
        df['var_acumulada'] = df['var_acumulada'].apply(lambda x: np.floor(x * 100) / 100)

        # Simular el reordenamiento del original si es necesario, 
        # pero el template original parece iterar fijo o por índice.
        return df, fecha_max

    def _obtener_variaciones_generales(self):
        query = "SELECT variacion_mensual, variacion_interanual FROM emae_variaciones ORDER BY fecha DESC LIMIT 1"
        df = pd.read_sql(query, self.engine)
        if df.empty: return 0.0, 0.0
        vm = np.floor(df['variacion_mensual'].iloc[0] * 100) / 100
        vi = np.floor(df['variacion_interanual'].iloc[0] * 100) / 100
        return vm, vi

    def _render_template(self, vm, vi, df_vars):
        base_path = os.path.dirname(os.path.abspath(__file__))
        root_path = os.path.join(base_path, '..')
        
        env = Environment(loader=FileSystemLoader([
            os.path.join(root_path, "correo_html_imagenes"),
            root_path
        ]))
        template = env.get_template('correo_html_imagenes/formato_emae.html')
        
        return template.render(var_mensual=vm, var_interanual=vi, df_variaciones=df_vars)

    def _enviar(self, html, fecha_txt):
        emisor = os.getenv('EMAIL_USER', 'departamientoactualizaciondato@gmail.com')
        pwd = os.getenv('EMAIL_PASS', 'cmxddbshnjqfehka')
        
        try:
            destinatarios = self._obtener_destinatarios()
        except:
            destinatarios = ['matizalazar2001@gmail.com'] # Fallback

        msg = MIMEMultipart('alternative')
        msg['Subject'] = f'Actualización de datos EMAE - {fecha_txt}'
        msg['From'] = emisor
        msg['To'] = ", ".join(destinatarios)

        # Imágenes
        html = self._adjuntar_imagenes(msg, html)
        msg.attach(MIMEText(html, 'html'))

        context = ssl.create_default_context()
        with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as server:
            server.login(emisor, pwd)
            server.sendmail(emisor, destinatarios, msg.as_string())

    def _obtener_destinatarios(self):
        engine_mail = self._get_engine_emails()
        try:
            df = pd.read_sql("SELECT email FROM correos", engine_mail)
            return df['email'].tolist()
        except:
            return ['matizalazar2001@gmail.com', 'manumarder@gmail.com']

    def _adjuntar_imagenes(self, msg, html):
        root_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
        img_dir = os.path.join(root_path, "correo_html_imagenes", "images")
        
        img_map = {
            'facebook2x': 'facebook2x.png',
            'twitter2x': 'twitter2x.png',
            'instagram2x': 'instagram2x.png'
        }

        for cid, filename in img_map.items():
            path = os.path.join(img_dir, filename)
            if os.path.exists(path):
                with open(path, 'rb') as f:
                    img = MIMEImage(f.read())
                    img.add_header('Content-ID', f'<{cid}>')
                    msg.attach(img)
                html = html.replace(f'src="images/{filename}"', f'src="cid:{cid}"')
        
        return html

    def _formatear_fecha(self, dt):
        meses = {
            1: 'Enero', 2: 'Febrero', 3: 'Marzo', 4: 'Abril',
            5: 'Mayo', 6: 'Junio', 7: 'Julio', 8: 'Agosto',
            9: 'Septiembre', 10: 'Octubre', 11: 'Noviembre', 12: 'Diciembre'
        }
        return f"{meses.get(dt.month, '')} de {dt.year}"
