#Importacion de Bibliotecas
import os
import sys
from Extraction_homePage import HomePage
from Transformation_super import Transformation_Data
from Load_super import conexionBaseDatos
from datos_deflactados import Deflactador

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)

# Crea una instancia de la clase "Credenciales"
from credenciales_bdd import Credenciales
instancia_credenciales = Credenciales('datalake_economico')


if __name__ == '__main__':

    #Descarga del archivo
    HomePage().descargar_archivo()

    #Obtencion del dataframe con formato solicitado
    df = Transformation_Data().contruccion_df()
    
    #Almacenamos los datos
    conexionBaseDatos(instancia_credenciales.host,instancia_credenciales.user,instancia_credenciales.password,instancia_credenciales.database).cargar_datos(df)

    #=== ZONA DE PRUEBA
    from email.message import EmailMessage
    from ssl import create_default_context
    from smtplib import SMTP_SSL

    #Declaramos email desde el que se envia, la contraseña de la api, y los correos receptores.
    email_emisor='departamientoactualizaciondato@gmail.com'
    email_contrasenia = 'cmxddbshnjqfehka'
    email_receptores =  ['benitezeliogaston@gmail.com']

    #==== Zona de envio de correo
    em = EmailMessage()
    em['From'] = email_emisor
    em['To'] = email_receptores
    em['Subject'] = "CORREO DE PRUEBA - SUPERMERCADO"
    em.set_content("Prueba de funcionamiento de SUPERMERCADO.")

    contexto= create_default_context()

    with SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
        smtp.login(email_emisor, email_contrasenia)
        smtp.sendmail(email_emisor, email_receptores, em.as_string())


