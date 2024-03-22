from homePage_CBT import HomePageCBT
from homePage_Pobreza import HomePagePobreza
from Transform_CbtCba import loadXLSDataCBT
from connectionDataBase import connection_db
from send_mail import MailCBTCBA
from connectIPECD_ECONOMICO import connection_db_ipecd_economico
import os
import sys 


# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Importar credenciales de la base de datos
from credenciales_bdd import Credenciales

# Después puedes crear una instancia de Credenciales
credenciales_datalake_sociodemografico = Credenciales('datalake_sociodemografico')
credenciales_ipecd_economico = Credenciales('ipecd_economico')


if __name__ == '__main__':
    # ZONA DE EXTRACT -- Donde se buscan los datos
    # Descargar archivos de HomePageCBT y HomePagePobreza
    home_page_CBT = HomePageCBT()
    home_page_CBT.descargar_archivo()

    home_page_Pobreza = HomePagePobreza()
    home_page_Pobreza.descargar_archivo()

    # Transformar datos del archivo Excel de HomePageCBT
    df = loadXLSDataCBT().transform_datalake()

    # Conexión y carga de datos en la base de datos IPECD Económico
    instancia_conexion_ipecd_economico = connection_db_ipecd_economico(credenciales_ipecd_economico.host, credenciales_ipecd_economico.user, credenciales_ipecd_economico.password, credenciales_ipecd_economico.database)
    instancia_conexion_ipecd_economico.connect_db()
    instancia_conexion_ipecd_economico.load_datalake(df)

    # Conexión y carga de datos en la base de datos del DataLake Sociodemográfico
    instancia_conexion_bdd = connection_db(credenciales_datalake_sociodemografico.host, credenciales_datalake_sociodemografico.user, credenciales_datalake_sociodemografico.password, credenciales_datalake_sociodemografico.database)
    instancia_conexion_bdd.connect_db()
    bandera_correo = instancia_conexion_bdd.load_datalake(df)

    # Si se cargaron los datos correctamente en el DataLake Sociodemográfico, enviar correo.
    if bandera_correo:
        instancia_correo = MailCBTCBA(credenciales_datalake_sociodemografico.host, credenciales_datalake_sociodemografico.user, credenciales_datalake_sociodemografico.password, "dwh_sociodemografico")
        instancia_correo.send_mail_cbt_cba()
