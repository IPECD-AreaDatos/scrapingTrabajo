from pymysql import connect
import pandas as pd
from calendar import month_name
from email.message import EmailMessage
from ssl import create_default_context
from smtplib import SMTP_SSL

class MailSipa:

    def __init__(self,host,user,password,database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def connect_db(self):
        self.conn = connect(
            host = self.host,
            user = self.user,
            password = self.password,
            database = self.database
        )
        self.cursor = self.conn.cursor()

    def close_conections(self):
        self.conn.commit()
        self.cursor.close()
        self.conn.close()

    def obtener_correos(self, modo="todos"):
        if modo == "matias":
            return ["matizalazar2001@gmail.com"]
        try:
            conn = connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database='ipecd_economico'
            )
            cursor = conn.cursor()
            cursor.execute("SELECT email FROM correos")
            correos = [row[0] for row in cursor.fetchall()]
            conn.close()
            return correos
        except Exception as e:
            print(f"❌ Error obteniendo correos: {e}")
            return []

    def extract_date_nation(self):
        conn = connect(
            host = self.host,
            user = self.user,
            password = self.password,
            database = 'dwh_economico'
        )
        cursor = conn.cursor()
        df = pd.read_sql("SELECT * FROM empleo_nacional_porcentajes_variaciones", conn)
        df_nea = pd.read_sql("SELECT * FROM empleo_nea_variaciones", conn)
        conn.close()
        return df, df_nea

    def send_mail(self):
        df, df_nea = self.extract_date_nation()

        df['fecha'] = pd.to_datetime(df['fecha'])
        df_nea['fecha'] = pd.to_datetime(df_nea['fecha'])

        df = df.sort_values('fecha')
        df_nea = df_nea.sort_values('fecha')

        fecha_max = df['fecha'].max()
        fecha_asunto = self.obtener_mes_actual(fecha_max) + " del " + str(fecha_max.year)

        diferencia_mensual = int((df['empleo_total'].iloc[-1] * 1000) - (df['empleo_total'].iloc[-2] * 1000))
        diferencia_interanual = int((df['empleo_total'].iloc[-1] - df['empleo_total'].iloc[-13]) * 1000)

        mensaje_uno = f'''
        <html>
        <body>
        <h2 style="font-size: 24px;"><strong> DATOS NUEVOS EN LA TABLA DE SISTEMA INTEGRADO PREVISIONAL ARGENTINO (SIPA) A {fecha_asunto.upper()}. </strong></h2>
        <hr>
        <h3> Distribucion de los Trabajos Registrados - Argentina </h3>
        <p>1 - Empleo privados registrados: <span style="font-size: 17px;"><b>{df['p_empleo_privado'].iloc[-1]:,.2f}%</b></span></p>
        <p>2 - Empleos publicos registrados: <span style="font-size: 17px;"><b>{df['p_empleo_publico'].iloc[-1]:,.2f}%</b></span></p>
        <p>3 - Monotributistas Independientes: <span style="font-size: 17px;"><b>{df['p_empleo_independiente_monotributo'].iloc[-1]:,.2f}%</b></span></p>
        <p>4 - Monotributistas Sociales: <span style="font-size: 17px;"><b>{df['p_empleo_monotributo_social'].iloc[-1]:,.2f}%</b></span></p>
        <p>5 - Empleo en casas particulares registrado: <span style="font-size: 17px;"><b>{df['p_empleo_casas_particulares'].iloc[-1]:,.2f}%</b></span></p>
        <p>6 - Trabajadores independientes autonomos: <span style="font-size: 17px;"><b>{df['p_empleo_independiente_autonomo'].iloc[-1]:,.2f}%</b></span></p>
        <hr>
        <h3> Trabajo Registrado a nivel nacional: </h3>
        <p>Total: <span style="font-size: 17px;"><b>{df['empleo_total'].iloc[-1] * 1000 :,.0f}</b></span></p>
        <p>Variacion mensual: <span style="font-size: 17px;"><b>{df['vmensual_empleo_total'].iloc[-1]:,.2f}%</b></span> ({diferencia_mensual:,.0f} puestos)  </p>
        <p>Variacion interanual: <span style="font-size: 17px;"><b>{df['vinter_empleo_total'].iloc[-1]:,.2f}%</b></span>  ({diferencia_interanual:,.0f} puestos)  </p>
        <hr>
        '''

        mensaje_dos = f'''
        <h3> TABLA DEL TRABAJO PRIVADO REGISTRADO </h3>
        <table style="border-collapse: collapse; width: 100%;">
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> GRUPO </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> TOTAL EMPLEO </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VARIACION MENSUAL </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> DIFERENCIA MENSUAL </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VARIACION INTERANUAL </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> DIFERENCIA INTERANUAL </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VARIACION ACUMULADA </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> DIFERENCIA ACUMULADA </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> PORCENTAJE REPRESENTATIVO EN EL NEA </th>
            <hr>
            {self.difference_by_province(df_nea)}
        </table>
        '''

        mensaje_tres = f'''
        <h3> TABLA DEL TRABAJO PRIVADO REGISTRADO </h3>
        <table style="border-collapse: collapse; width: 100%;">
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> GRUPO </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> TOTAL EMPLEO </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VARIACION MENSUAL </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> DIFERENCIA MENSUAL </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VARIACION INTERANUAL </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> DIFERENCIA INTERANUAL </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> VARIACION ACUMULADA </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> DIFERENCIA ACUMULADA </th>
            <th style="border: 1px solid #dddddd; text-align: left; padding: 8px;"> PORCENTAJE REPRESENTATIVO EN EL NEA </th>
            {self.difference_nea_nation(df_nea,df)}
        </table>
        <hr>
        '''

        mensaje_cuatro = self.max_hitory_corr_nation(df_nea,df)

        mensaje_cinco = ''' 
        <hr>
        <p> Instituto Provincial de Estadistica y Ciencia de Datos de Corrientes<br>
        Dirección: Tucumán 1164 - Corrientes Capital<br>
        Contacto Coordinación General: 3794 284993</p>
        </body>
        </html>
        '''

        cadena = mensaje_uno + mensaje_dos + mensaje_tres + mensaje_cuatro + mensaje_cinco

        email_emisor='departamientoactualizaciondato@gmail.com'
        email_contrasenia = 'cmxddbshnjqfehka'
        email_receptores = self.obtener_correos(modo="matias")

        em = EmailMessage()
        em['From'] = email_emisor
        em['To'] = ", ".join(email_receptores)
        em['Subject'] = f'SISTEMA INTEGRADO PREVISIONAL ARGENTINO(SIPA) - {fecha_asunto}'
        em.set_content(cadena, subtype = 'html')

        contexto= create_default_context()
        with SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
            smtp.login(email_emisor, email_contrasenia)
            smtp.sendmail(email_emisor, email_receptores, em.as_string())

    def obtener_mes_actual(self,fecha_ultimo_registro):
        nombre_mes_ingles = month_name[fecha_ultimo_registro.month]
        traducciones_meses = {
            'January': 'Enero', 'February': 'Febrero', 'March': 'Marzo',
            'April': 'Abril', 'May': 'Mayo', 'June': 'Junio',
            'July': 'Julio', 'August': 'Agosto', 'September': 'Septiembre',
            'October': 'Octubre', 'November': 'Noviembre', 'December': 'Diciembre'
        }
        return traducciones_meses.get(nombre_mes_ingles, nombre_mes_ingles)
