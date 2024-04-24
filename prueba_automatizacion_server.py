from email.message import EmailMessage
from ssl import create_default_context
from smtplib import SMTP_SSL
from selenium import webdriver

# Configuración del navegador
options = webdriver.ChromeOptions()
options.add_argument('--headless')

driver = webdriver.Chrome(options=options)  # Reemplaza con la ubicación de tu ChromeDriver

# URL de la página que deseas obtener
url_pagina = 'https://www.argentina.gob.ar/trabajo/estadisticas'

# Acceder a la página
driver.get(url_pagina)

# Obtener el título de la página
titulo_pagina = driver.title

# Imprimir el título en la consola
print("El título de la página es:", titulo_pagina)

#Declaramos email desde el que se envia, la contraseña de la api, y los correos receptores.
email_emisor='departamientoactualizaciondato@gmail.com'
email_contrasenia = 'cmxddbshnjqfehka'
email_receptores =  ['benitezeliogaston@gmail.com']

#==== Zona de envio de correo
em = EmailMessage()
em['From'] = email_emisor
em['To'] = email_receptores
em['Subject'] = "CORREO DE PRUEBA"
em.set_content("Esto es una prueba de la automatizacion de AWS")

contexto= create_default_context()

with SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
    smtp.login(email_emisor, email_contrasenia)
    smtp.sendmail(email_emisor, email_receptores, em.as_string())