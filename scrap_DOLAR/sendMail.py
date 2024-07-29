import pandas as pd
import smtplib
from email.message import EmailMessage
from datetime import datetime

class SendMail:
    def send_mail(self, df_oficial, df_blue, df_mep, df_ccl):
        email_emisor = 'departamientoactualizaciondato@gmail.com'
        email_contraseña = 'cmxddbshnjqfehka'
        email_receptores = [
            'benitezeliogaston@gmail.com', 
            'matizalazar2001@gmail.com', 
            'manumarder@gmail.com'
        ]

        # Obtener la fecha actual en el formato deseado
        fecha_actual = datetime.now().strftime("%d de %B")
        
        # Crear un objeto EmailMessage
        msg = EmailMessage()
        msg['Subject'] = f'Cotizaciones Dolares - {fecha_actual}'
        msg['From'] = email_emisor
        msg['To'] = ', '.join(email_receptores)

        # Convertir los DataFrames a HTML
        df_oficial_html = df_oficial.to_html(index=False)
        df_blue_html = df_blue.to_html(index=False)
        df_mep_html = df_mep.to_html(index=False)
        df_ccl_html = df_ccl.to_html(index=False)

        # Crear el cuerpo del correo electrónico
        body = f"""\
        <html>
            <body>
                <p>Las cotizaciones del dolar oficial, blue, mep y ccl son las siguientes:</p>
                <h2>Dolar Oficial</h2>
                {df_oficial_html}
                <h2>Dolar Blue</h2>
                {df_blue_html}
                <h2>Dolar MEP</h2>
                {df_mep_html}
                <h2>Dolar CCL</h2>
                {df_ccl_html}
            </body>
        </html>
        """

        # Agregar el cuerpo al correo electrónico
        msg.set_content(body, subtype='html')

        # Enviar el correo electrónico usando un contexto de administrador con SSL
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(email_emisor, email_contraseña)
            smtp.send_message(msg)