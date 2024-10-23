from homePage_CBT import HomePageCBT
from homePage_Pobreza import HomePagePobreza
from Transform_CbtCba import loadXLSDataCBT
from connectionDataBase import connection_db
from send_mail import MailCBTCBA
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

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_SOCIO'))
dbb_dwh = (os.getenv('NAME_DBB_DWH_SOCIO'))

if __name__ == '__main__':
    # ZONA DE EXTRACT -- Donde se buscan los datos
    # Descargar archivos de HomePageCBT y HomePagePobreza
    home_page_CBT = HomePageCBT()
    home_page_CBT.descargar_archivo()

    home_page_Pobreza = HomePagePobreza()
    home_page_Pobreza.descargar_archivo()

    # Transformar datos del archivo Excel de HomePageCBT
    df = loadXLSDataCBT().transform_datalake()
    
    # Conexión y carga de datos en la base de datos del DataLake Sociodemográfico
    instancia_conexion_bdd = connection_db(host_dbb,user_dbb,pass_dbb,dbb_datalake)
    instancia_conexion_bdd.connect_db()
    bandera_correo = instancia_conexion_bdd.load_datalake(df)
    # Si se cargaron los datos correctamente en el DataLake Sociodemográfico, enviar correo.
    if bandera_correo:
        instancia_correo = MailCBTCBA(host_dbb,user_dbb,pass_dbb,dbb_dwh)
        instancia_correo.send_mail_cbt_cba()

