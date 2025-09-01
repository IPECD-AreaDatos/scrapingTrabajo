import pandas as pd
from calendar import month_name
from email.message import EmailMessage
from ssl import create_default_context
from smtplib import SMTP_SSL
from pymysql import connect
from dotenv import load_dotenv
import os


class MailSipa:
    def __init__(self, host, user, password, database):
        load_dotenv()
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.email_emisor = os.getenv("EMAIL_EMISOR")
        self.email_contrasenia = os.getenv("EMAIL_PASSWORD")

    def get_connection(self, db_name):
        return connect(
            host=self.host, user=self.user, password=self.password, database=db_name
        )

    def obtener_correos(self, modo="todos"):
        if modo == "matias":
            return ["matizalazar2001@gmail.com", "manumarder@gmail.com"]

        try:
            with self.get_connection("ipecd_economico") as conn:
                df = pd.read_sql("SELECT email FROM correos", conn)
                return df["email"].dropna().tolist()
        except Exception as e:
            print(f"❌ Error obteniendo correos: {e}")
            return []

    def extract_date_nation(self):
        with self.get_connection("dwh_economico") as conn:
            df = pd.read_sql(
                "SELECT * FROM empleo_nacional_porcentajes_variaciones", conn
            )
            df_nea = pd.read_sql("SELECT * FROM empleo_nea_variaciones", conn)
        return df, df_nea

    def obtener_mes_actual(self, fecha_ultimo_registro):
        nombre_mes_ingles = month_name[fecha_ultimo_registro.month]
        traducciones_meses = {
            "January": "Enero",
            "February": "Febrero",
            "March": "Marzo",
            "April": "Abril",
            "May": "Mayo",
            "June": "Junio",
            "July": "Julio",
            "August": "Agosto",
            "September": "Septiembre",
            "October": "Octubre",
            "November": "Noviembre",
            "December": "Diciembre",
        }
        return traducciones_meses.get(nombre_mes_ingles, nombre_mes_ingles)

    def difference_by_province(self, df_nea):
        provincias = {
            'corrientes': 'Corrientes',
            'misiones': 'Misiones',
            'chaco': 'Chaco',
            'formosa': 'Formosa',
            'nea': 'NEA Total'
        }

        def format_pct(val):
            if pd.isna(val):
                return "-"
            if abs(val) < 0.0001:
                return "0.00%"
            return f"{val:.2f}%"
        def format_num(val):
            if pd.isna(val):
                return "-"
            return f"{int(val):,}"

        filas_html = ""

        # Tomar la última fila que tenga variación acumulada (vacum_nea) no nula
        ultima_fila = df_nea[df_nea["vacum_nea"].notna()].iloc[-1]

        for prov in provincias.keys():
            total = ultima_fila.get(f"total_{prov}", None)
            v_mensual = ultima_fila.get(f"vmensual_{prov}", None)
            v_inter = ultima_fila.get(f"vinter_{prov}", None)
            v_acum = ultima_fila.get(f"vacum_{prov}", None)
            dif_mensual = ultima_fila.get(f"dif_mensual_{prov}", None)
            dif_inter = ultima_fila.get(f"dif_inter_{prov}", None)
            dif_acum = ultima_fila.get(f"dif_acum_{prov}", None)
            porc_nea = (total / ultima_fila["total_nea"] * 100) if pd.notna(total) and ultima_fila["total_nea"] > 0 else None

            filas_html += f"""
            <tr>
                <td style="border: 1px solid #ddd; padding: 6px;">{provincias[prov]}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">{format_num(total)}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">{format_pct(v_mensual)}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">{format_num(dif_mensual)}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">{format_pct(v_inter)}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">{format_num(dif_inter)}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">{format_pct(v_acum)}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">{format_num(dif_acum)}</td>
                <td style="border: 1px solid #ddd; padding: 6px;">{format_pct(porc_nea)}</td>
            </tr>
            """
        return filas_html


    def difference_nea_nation(self, df_nea, df):
        """
        Devuelve filas HTML comparando NEA y Nación en la última fecha.
        """
        filas = ""

        # Última fila NEA con variación acumulada
        ultima_fila_nea = df_nea[df_nea["vacum_nea"].notna()].iloc[-1]

        # Última fila Nación con variación acumulada
        ultima_fila_nacion = df[df["vacum_empleo_total"].notna()].iloc[-1]

        # Empleo total NEA y Nación
        total_nea = ultima_fila_nea["total_nea"]
        total_nacion = ultima_fila_nacion["empleo_total"] * 1000
        porcentaje_nea_nacion = (total_nea / total_nacion) * 100 if total_nacion > 0 else 0

        # Variaciones NEA
        var_mensual_nea = ultima_fila_nea["vmensual_nea"]
        var_inter_nea = ultima_fila_nea["vinter_nea"]
        vacum_nea = ultima_fila_nea["vacum_nea"]
        dif_mensual_nea = ultima_fila_nea["dif_mensual_nea"]
        dif_inter_nea = ultima_fila_nea["dif_inter_nea"]
        dif_acum_nea = ultima_fila_nea["dif_acum_nea"]

        # Variaciones Nación
        var_mensual_nacion = ultima_fila_nacion["vmensual_empleo_total"]
        var_inter_nacion = ultima_fila_nacion["vinter_empleo_total"]
        vacum_nacion = ultima_fila_nacion["vacum_empleo_total"]

        # Para diferencia acumulada nacional usamos el cálculo aproximado
        dif_mensual_nacion = ultima_fila_nacion["dif_mensual_empleo_total"]
        dif_inter_nacion = ultima_fila_nacion["dif_inter_empleo_total"]
        dif_acum_nacion = ultima_fila_nacion["dif_acum_empleo_total"]


        def format_pct(val):
            if pd.isna(val):
                return "-"
            return f"{val:.2f}%"

        def format_num(val):
            if pd.isna(val):
                return "-"
            return f"{int(val):,}"

        filas += f"""
        <tr>
            <td style="border:1px solid #ddd;padding:6px;"><b>NEA Total</b></td>
            <td style="border:1px solid #ddd;padding:6px;">{format_num(total_nea)}</td>
            <td style="border:1px solid #ddd;padding:6px;">{format_pct(var_mensual_nea)}</td>
            <td style="border:1px solid #ddd;padding:6px;">{format_num(dif_mensual_nea)}</td>
            <td style="border:1px solid #ddd;padding:6px;">{format_pct(var_inter_nea)}</td>
            <td style="border:1px solid #ddd;padding:6px;">{format_num(dif_inter_nea)}</td>
            <td style="border:1px solid #ddd;padding:6px;">{format_pct(vacum_nea)}</td>
            <td style="border:1px solid #ddd;padding:6px;">{format_num(dif_acum_nea)}</td>
            <td style="border:1px solid #ddd;padding:6px;">{format_pct(porcentaje_nea_nacion)} del total nacional</td>
        </tr>
        """

        filas += f"""
        <tr>
            <td colspan="9" style="border:1px solid #ddd;padding:6px;">
                Comparativa de variaciones: NEA vs Nación - Último mes
            </td>
        </tr>
        <tr>
            <td style="border:1px solid #ddd;padding:6px;"><b>Nación</b></td>
            <td style="border:1px solid #ddd;padding:6px;">{format_num(total_nacion)}</td>
            <td style="border:1px solid #ddd;padding:6px;">{format_pct(var_mensual_nacion)}</td>
            <td style="border:1px solid #ddd;padding:6px;">{format_num(dif_mensual_nacion)}</td>
            <td style="border:1px solid #ddd;padding:6px;">{format_pct(var_inter_nacion)}</td>
            <td style="border:1px solid #ddd;padding:6px;">{format_num(dif_inter_nacion)}</td>
            <td style="border:1px solid #ddd;padding:6px;">{format_pct(vacum_nacion)}</td>
            <td style="border:1px solid #ddd;padding:6px;">{format_num(dif_acum_nacion)}</td>
            <td style="border:1px solid #ddd;padding:6px;">100%</td>
        </tr>
        """

        return filas


    def max_hitory_corr_nation(self, df_nea, df):
        max_privado = df["empleo_total"].max() * 1000
        fecha_max = df.loc[df["empleo_total"].idxmax(), "fecha"].strftime("%B %Y")
        return f"""
        <h3>Máximo histórico de empleo registrado</h3>
        <p>El mayor valor registrado fue de <b>{max_privado:,.0f}</b> empleos en {fecha_max}.</p>
        """

    def generar_html(
        self, df, df_nea, fecha_asunto, diferencia_mensual, diferencia_interanual
    ):
        mensaje_uno = f"""
        <html>
        <body>
        <h2 style="font-size: 24px;"><strong>DATOS NUEVOS EN LA TABLA DE SISTEMA INTEGRADO PREVISIONAL ARGENTINO (SIPA) A {fecha_asunto.upper()}.</strong></h2>
        <hr>
        <h3>Distribución de los Trabajos Registrados - Argentina</h3>
        <p>1 - Empleo privados registrados: <span style="font-size: 17px;"><b>{df['p_empleo_privado'].iloc[-1]:,.2f}%</b></span></p>
        <p>2 - Empleos públicos registrados: <span style="font-size: 17px;"><b>{df['p_empleo_publico'].iloc[-1]:,.2f}%</b></span></p>
        <p>3 - Monotributistas Independientes: <span style="font-size: 17px;"><b>{df['p_empleo_independiente_monotributo'].iloc[-1]:,.2f}%</b></span></p>
        <p>4 - Monotributistas Sociales: <span style="font-size: 17px;"><b>{df['p_empleo_monotributo_social'].iloc[-1]:,.2f}%</b></span></p>
        <p>5 - Empleo en casas particulares registrado: <span style="font-size: 17px;"><b>{df['p_empleo_casas_particulares'].iloc[-1]:,.2f}%</b></span></p>
        <p>6 - Trabajadores independientes autónomos: <span style="font-size: 17px;"><b>{df['p_empleo_independiente_autonomo'].iloc[-1]:,.2f}%</b></span></p>
        <hr>
        <h3>Trabajo Registrado a nivel nacional:</h3>
        <p>Total: <span style="font-size: 17px;"><b>{df['empleo_total'].iloc[-1]*1000:,.0f}</b></span></p>
        <p>Variación mensual: <span style="font-size: 17px;"><b>{df['vmensual_empleo_total'].iloc[-1]:,.2f}%</b></span> ({diferencia_mensual:,.0f} puestos)</p>
        <p>Variación interanual: <span style="font-size: 17px;"><b>{df['vinter_empleo_total'].iloc[-1]:,.2f}%</b></span> ({diferencia_interanual:,.0f} puestos)</p>
        <hr>
        """

        mensaje_dos = f"""
        <h3>TABLA DEL TRABAJO PRIVADO REGISTRADO</h3>
        <table style="border-collapse: collapse; width: 100%;">
            <tr>
                <th style="border: 1px solid #dddddd; padding: 8px;">GRUPO</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">TOTAL EMPLEO</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">VARIACIÓN MENSUAL</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">DIF. MENSUAL</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">VARIACIÓN INTERANUAL</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">DIF. INTERANUAL</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">VARIACIÓN ACUMULADA</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">DIF. ACUMULADA</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">% REPRESENTATIVO EN EL NEA</th>
            </tr>
            {self.difference_by_province(df_nea)}
        </table>
        """

        mensaje_tres = f"""
        <h3>TABLA DEL TRABAJO PRIVADO REGISTRADO</h3>
        <table style="border-collapse: collapse; width: 100%;">
            <tr>
                <th style="border: 1px solid #dddddd; padding: 8px;">GRUPO</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">TOTAL EMPLEO</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">VARIACIÓN MENSUAL</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">DIF. MENSUAL</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">VARIACIÓN INTERANUAL</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">DIF. INTERANUAL</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">VARIACIÓN ACUMULADA</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">DIF. ACUMULADA</th>
                <th style="border: 1px solid #dddddd; padding: 8px;">% REPRESENTATIVO EN EL NEA</th>
            </tr>
            {self.difference_nea_nation(df_nea, df)}
        </table>
        <hr>
        """

        mensaje_cuatro = self.max_hitory_corr_nation(df_nea, df)

        mensaje_cinco = """
        <hr>
        <p>Instituto Provincial de Estadística y Ciencia de Datos de Corrientes<br>
        Dirección: Tucumán 1164 - Corrientes Capital<br>
        Contacto Coordinación General: 3794 284993</p>
        </body>
        </html>
        """

        return mensaje_uno + mensaje_dos + mensaje_tres + mensaje_cuatro + mensaje_cinco

    def send_mail(self):
        df, df_nea = self.extract_date_nation()
        df["fecha"] = pd.to_datetime(df["fecha"])
        df_nea["fecha"] = pd.to_datetime(df_nea["fecha"])

        df = df.sort_values("fecha")
        df_nea = df_nea.sort_values("fecha")

        fecha_max = df["fecha"].max()
        fecha_asunto = self.obtener_mes_actual(fecha_max) + " del " + str(fecha_max.year)

        diferencia_mensual = int(
            (df["empleo_total"].iloc[-1] * 1000) - (df["empleo_total"].iloc[-2] * 1000)
        )
        diferencia_interanual = int(
            (df["empleo_total"].iloc[-1] - df["empleo_total"].iloc[-13]) * 1000
        )

        html = self.generar_html(df, df_nea, fecha_asunto, diferencia_mensual, diferencia_interanual)
        receptores = self.obtener_correos(modo="matias")

        em = EmailMessage()
        em["From"] = self.email_emisor
        em["To"] = ", ".join(receptores)
        em["Subject"] = f"SIPA - {fecha_asunto}"
        em.set_content(html, subtype="html")

        contexto = create_default_context()
        with SMTP_SSL("smtp.gmail.com", 465, context=contexto) as smtp:
            smtp.login(self.email_emisor, self.email_contrasenia)
            smtp.send_message(em)

        print("✅ Correo enviado correctamente.")
