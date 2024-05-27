from extract import HomePage
from ripte_cargaHistorico import ripte_cargaHistorico
from ripte_cargaUltimoDato import ripte_cargaUltimoDato
import sys
import os

# Obtener la ruta al directorio que contiene las Credenciales
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, "..", "Credenciales_folder")
sys.path.append(credenciales_dir)
from credenciales_bdd import Credenciales

# Credenciales de las bases de datos a utilizar
credenciales_datalake_economico = Credenciales("datalake_economico")
credenciales_ipecd_economico = Credenciales("ipecd_economico")


if __name__ == "__main__":
    # Descarga del archivo
    HomePage().descargar_archivo()
    # ripte_cargaHistorico().loadInDataBase(credenciales_datalake_economico.host, credenciales_datalake_economico.user, credenciales_datalake_economico.password, credenciales_datalake_economico.database)
    
    # ↓↓↓↓↓↓↓↓↓↓↓↓CARGA DEL TABLERO ↓↓↓↓↓↓↓↓↓↓↓↓
    # ripte_cargaHistorico().loadInDataBase(credenciales_ipecd_economico.host, credenciales_ipecd_economico.user, credenciales_ipecd_economico.password, credenciales_ipecd_economico.database)

    # Scrip principal
    # Obtenemos el ultimo valor de RIPTE desde la pagina de inicio
    ultimo_valor_ripte = HomePage().extract_last_date()
    print(ultimo_valor_ripte)
    # Carga del último dato en la base de datos Datalake Economico
    instancia = ripte_cargaUltimoDato(
        credenciales_datalake_economico.host,
        credenciales_datalake_economico.user,
        credenciales_datalake_economico.password,
        credenciales_datalake_economico.database,
    )
    instancia.loadInDataBaseDatalakeEconomico()

    # ↓↓↓↓↓↓↓↓↓↓↓↓CARGA DEL TABLERO ↓↓↓↓↓↓↓↓↓↓↓↓
    # Carga del último dato en la base de datos IPECD Economico
    instancia = ripte_cargaUltimoDato(
        credenciales_ipecd_economico.host,
        credenciales_ipecd_economico.user,
        credenciales_ipecd_economico.password,
        credenciales_ipecd_economico.database,
    )
    instancia.loadInDataBaseIPECD_Economico()



    #=== ZONA DE PRUEBA
    from email.message import EmailMessage
    from ssl import create_default_context
    from smtplib import SMTP_SSL
    from selenium import webdriver


    #Declaramos email desde el que se envia, la contraseña de la api, y los correos receptores.
    email_emisor='departamientoactualizaciondato@gmail.com'
    email_contrasenia = 'cmxddbshnjqfehka'
    email_receptores =  ['benitezeliogaston@gmail.com']

    #==== Zona de envio de correo
    em = EmailMessage()
    em['From'] = email_emisor
    em['To'] = email_receptores
    em['Subject'] = "CORREO DE PRUEBA - RIPTE"
    em.set_content("Prueba de funcionamiento de RIPTE.")

    contexto= create_default_context()

    with SMTP_SSL('smtp.gmail.com', 465, context=contexto) as smtp:
        smtp.login(email_emisor, email_contrasenia)
        smtp.sendmail(email_emisor, email_receptores, em.as_string())