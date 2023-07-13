from loadHTML_TablaAutoInscripcionNacion import loadHTML_TablaAutoInscripcionNacion
from loadHTML_TablaMotoInscripcionNacion import loadHTML_TablaMotoInscripcionNacion
from loadHTML_TablaAutoInscripcionCorrientes import loadHTML_TablaAutoInscripcionCorrientes
from loadHTML_TablaMotoInscripcionCorrientes import loadHTML_TablaMotoInscripcionCorrientes
from loadHTML_TablaParqueActivoNacion import loadHTML_TablaParqueActivoNacion
from email.message import EmailMessage
import ssl
import smtplib

#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

# Lista de tablas a cargar
inscripcion = [
    loadHTML_TablaAutoInscripcionNacion,
    loadHTML_TablaMotoInscripcionNacion,
    loadHTML_TablaAutoInscripcionCorrientes,
    loadHTML_TablaMotoInscripcionCorrientes,
    loadHTML_TablaParqueActivoNacion

]

if __name__ == '__main__':
    for registroPropiedadAutomotor in inscripcion:
        registroPropiedadAutomotor().loadInDataBase(host, user, password, database)
        
        
def enviar_correo():
    email_emisor='departamientoactualizaciondato@gmail.com'
    email_contraseña = 'oxadnhkcyjnyibao'
    email_receptor = 'gastongrillo2001@gmail.com'
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

input("Proceso completado - Presione Enter.")