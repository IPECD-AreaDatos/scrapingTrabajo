from email.message import EmailMessage
import ssl
import smtplib

def enviar_correo():
    email_emisor='matizalazar2001@gmail.com'
    email_contraseña = 'idlxnffjuqpuspup'
    email_receptor = 'matizalazar2001@gmail.com'
    asunto = 'Modificación en la base de datos'
    mensaje = 'Se ha producido una modificación en la base de datos.'
    
    em = EmailMessage()
    em['From'] = email_emisor
    em['To'] = email_receptor
    em['Subject'] = asunto
    em.set_content(mensaje)
    
    contexto= ssl.create_default_context()
    
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
        smtp.login(email_emisor, email_contraseña)
        smtp.sendmail(email_emisor, email_receptor, em.as_string())

enviar_correo()   