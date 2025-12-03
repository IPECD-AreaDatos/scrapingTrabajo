"""
LOAD - Módulo de carga y reporte IPC
Responsabilidad: Cargar a MySQL y enviar correo de reporte
"""
import logging
import os
import calendar
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import pymysql

# Librerías de correo
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from ssl import create_default_context
from smtplib import SMTP_SSL

logger = logging.getLogger(__name__)

class LoadIPC:
    
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:3306/{self.database}"
        self.engine = create_engine(self.connection_string)

    def load_to_db(self, df):
        """
        Carga datos nuevos a la BDD.
        Returns: True si hubo datos nuevos, False si no.
        """
        try:
            logger.info("Iniciando verificación de datos en BD...")
            
            # Obtener registros existentes (fecha, id_region) para filtrar rápido
            query = "SELECT fecha, id_region FROM ipc_valores"
            registros_bdd = pd.read_sql(query, self.engine)
            
            # Crear set de tuplas para búsqueda O(1)
            # Asegurar que las fechas sean del mismo tipo
            registros_bdd['fecha'] = pd.to_datetime(registros_bdd['fecha'])
            set_existentes = set(zip(registros_bdd['fecha'], registros_bdd['id_region']))
            
            # Filtrar nuevos
            # Convertimos fecha df a datetime por seguridad
            df['fecha'] = pd.to_datetime(df['fecha'])
            
            nuevos_registros = df[~df.apply(lambda x: (x['fecha'], x['id_region']) in set_existentes, axis=1)]

            logger.info(f"Registros en BD: {len(registros_bdd)}")
            logger.info(f"Registros extraídos: {len(df)}")
            logger.info(f"Nuevos registros a insertar: {len(nuevos_registros)}")

            if not nuevos_registros.empty:
                logger.info("Insertando nuevos registros...")
                nuevos_registros.to_sql(name='ipc_valores', con=self.engine, if_exists='append', index=False)
                logger.info("[OK] Base de datos actualizada.")
                return True
            else:
                logger.info("[SKIP] No hay datos nuevos.")
                return False

        except Exception as e:
            logger.error(f"Error en carga a BD: {e}")
            raise

    def enviar_reporte(self):
        """Genera y envía el correo con el reporte del IPC (Lógica de correo2.py)"""
        try:
            logger.info("Preparando envío de correo de reporte...")
            
            # 1. Obtener última fecha
            fecha_max = pd.read_sql("SELECT MAX(fecha) as fecha FROM ipc_valores", self.engine).iloc[0]['fecha']
            fecha_str = str(fecha_max)
            logger.info(f"Generando reporte para fecha: {fecha_str}")

            # 2. Obtener datos para el reporte
            var_mensual, var_interanual, var_acumulada = self._get_variaciones_nacion(fecha_str)
            df_var_region = self._get_variaciones_region(fecha_str)
            df_var_nea = self._get_variaciones_nea(fecha_str)

            # 3. Construir HTML (Lógica original de correo2.py adaptada)
            cuerpo_html = self._construir_html(fecha_max, var_mensual, var_interanual, var_acumulada, df_var_region, df_var_nea)

            # 4. Enviar Correo
            self._enviar_email_final(cuerpo_html, fecha_max)
            logger.info("[OK] Correo enviado exitosamente.")

        except Exception as e:
            logger.error(f"Error enviando reporte: {e}")
            raise

    # ================= MÉTODOS PRIVADOS DE CORREO =================

    def _get_variaciones_nacion(self, fecha):
        query = f"SELECT * FROM ipc_valores WHERE id_region = 1 AND id_categoria = 1 AND fecha = '{fecha}'"
        df = pd.read_sql(query, self.engine)
        if not df.empty:
            return (df.iloc[0]['var_mensual'] * 100, 
                    df.iloc[0]['var_interanual'] * 100, 
                    df.iloc[0]['var_acumulada'] * 100)
        return None, None, None

    def _get_variaciones_region(self, fecha):
        query = f"SELECT * FROM ipc_valores WHERE id_categoria = 1 AND fecha = '{fecha}' ORDER BY var_mensual DESC"
        df = pd.read_sql(query, self.engine)
        
        # Mapeo regiones
        regiones = pd.read_sql("SELECT id_region, descripcion_region FROM identificador_regiones", self.engine)
        mapa = dict(zip(regiones['id_region'], regiones['descripcion_region']))
        df['id_region'] = df['id_region'].map(mapa)
        
        # Formato
        for col in ['var_mensual', 'var_interanual', 'var_acumulada']:
            df[col] = (df[col] * 100).apply(lambda x: f"{x:.2f}%")
        
        return df[['id_region', 'var_mensual', 'var_interanual', 'var_acumulada']]

    def _get_variaciones_nea(self, fecha):
        subs = [1,2,14,17,20,25,27,30,35,37,41,42,44]
        query = f"""
            SELECT id_categoria, var_mensual, var_interanual, var_acumulada 
            FROM ipc_valores 
            WHERE fecha = '{fecha}' AND id_region = 5 AND id_subdivision IN ({','.join(map(str, subs))})
        """
        df = pd.read_sql(query, self.engine)
        
        # Mapeo categorias
        cats = pd.read_sql("SELECT id_categoria, nombre FROM ipc_categoria", self.engine)
        mapa = dict(zip(cats['id_categoria'], cats['nombre']))
        df['id_categoria'] = df['id_categoria'].map(mapa)
        
        # Formato
        for col in ['var_mensual', 'var_interanual', 'var_acumulada']:
            df[col] = (df[col] * 100).apply(lambda x: f"{x:.2f}%")
            
        return df.sort_values('var_mensual', ascending=False)

    def _construir_html(self, fecha, vm, vi, va, df_reg, df_nea):
        # ... (Aquí va TODA la lógica HTML de tu correo2.py dentro de un f-string)
        # Por brevedad, asumo que copias y pegas la estructura HTML de tu correo2.py
        # reemplazando las variables.
        
        # Helper fecha
        meses = {1:'Enero', 2:'Febrero', 3:'Marzo', 4:'Abril', 5:'Mayo', 6:'Junio',
                 7:'Julio', 8:'Agosto', 9:'Septiembre', 10:'Octubre', 11:'Noviembre', 12:'Diciembre'}
        fecha_texto = f"{meses[fecha.month]} del {fecha.year}"

        html = f"""
        <html>
        <head>
            <style>
                /* ... Estilos de tu correo2.py ... */
                @media (max-width: 600px) {{ .box-mapa {{ flex-direction: column !important; }} }}
            </style>
        </head>
        <body style="color-scheme: light;">
            <div class="container" style="background-image: url('cid:fondo'); background-size: cover;">
                <h2 style="text-align: center; color: #172147;">DATOS NUEVOS IPC A {fecha_texto.upper()}</h2>
                
                <div class="box" style="background-image: url('cid:fondo_var'); text-align: center; padding: 20px;">
                    <h4 style="color: white;">MENSUAL: {vm:.2f}% | INTERANUAL: {vi:.2f}% | ACUMULADA: {va:.2f}%</h4>
                </div>

                <div style="display: flex; margin: 20px;">
                    <img src="cid:mapa_regiones" style="width: 200px; height: auto;">
                    <table style="width: 100%; border-collapse: collapse;">
                        <tr style="background-color: #313A63; color: white;">
                            <th>Región</th><th>Mes</th><th>Año</th><th>Acum</th>
                        </tr>
        """
        # Filas Regiones
        for _, row in df_reg.iterrows():
            html += f"<tr><td>{row['id_region']}</td><td>{row['var_mensual']}</td><td>{row['var_interanual']}</td><td>{row['var_acumulada']}</td></tr>"

        html += """
                    </table>
                </div>

                <h2 style="text-align: center;">Datos del NEA</h2>
                <table style="width: 95%; margin: auto; border-collapse: collapse;">
                    <tr style="background-color: #313A63; color: white;">
                        <th>Subdivisión</th><th>Mes</th><th>Año</th><th>Acum</th>
                    </tr>
        """
        # Filas NEA
        for _, row in df_nea.iterrows():
            html += f"<tr><td>{row['id_categoria']}</td><td>{row['var_mensual']}</td><td>{row['var_interanual']}</td><td>{row['var_acumulada']}</td></tr>"

        html += """
                </table>
                <div style="text-align: center; margin-top: 20px;">
                    <img src="cid:ipecd" style="width: 200px;">
                </div>
            </div>
        </body>
        </html>
        """
        return html

    def _enviar_email_final(self, html_content, fecha):
        # Credenciales
        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_pass = 'cmxddbshnjqfehka' # (Considera mover esto al .env)
        
        # Obtener destinatarios
        #destinatarios = pd.read_sql("SELECT email FROM correos WHERE prueba = 1", self.engine)['email'].tolist()
        
        destinatarios = 'manumarder@gmail.com'

        msg = MIMEMultipart('related')
        msg['From'] = email_emisor
        msg['To'] = ', '.join(destinatarios)
        msg['Subject'] = f'Actualizacion de datos IPC - {fecha.date()}'
        
        msg.attach(MIMEText(html_content, 'html'))
        
        # Adjuntar imágenes
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # scrap_IPC/
        img_dir = os.path.join(base_dir, 'files')
        
        imagenes = {
            "ipecd": "logo_ipecd.png", 
            "fondo": "fondo_correo.png",
            "fondo_var": "fondo_var.png",
            "mapa_regiones": "mapa_regiones.png"
        }

        for cid, filename in imagenes.items():
            path = os.path.join(img_dir, filename)
            if os.path.exists(path):
                with open(path, 'rb') as f:
                    img = MIMEImage(f.read())
                    img.add_header('Content-ID', f'<{cid}>')
                    msg.attach(img)
            else:
                logger.warning(f"Imagen no encontrada: {path}")

        contexto = create_default_context()
        with SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_pass)
            smtp.sendmail(email_emisor, destinatarios, msg.as_string())