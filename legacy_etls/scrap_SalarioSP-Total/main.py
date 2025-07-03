from homePage import HomePage
from loadCSVData_SP import Gestion_bdd
import sys
import os
import transform

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))

if __name__ == '__main__':
    home_page = HomePage()
    home_page.descargar_archivo()

    #Obtencion de los datos con el formato correcto 
    df_salario_sp = transform.datos_sp() #--> Datos del PRIVADO
    df_datos_totales = transform.datos_totales() #--> Datos TOTALES
    
    #Creamos instancia para cargar datos en el DATALAKE
    instancia = Gestion_bdd(host_dbb,user_dbb,pass_dbb,dbb_datalake)

    #Carga de DATALAKE de datos de Salario Privado
    instancia.loadInDataBase(df_salario_sp,'dp_salarios_sector_privado')
    instancia.loadInDataBase(df_datos_totales,'dp_salarios_total')



