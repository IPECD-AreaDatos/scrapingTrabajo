from extract import HomePage
import os
from transform import Transformer
from load import Load
import os
from dotenv import load_dotenv
import sys 

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))

#Rama principal de ejecucion
def main():

    #Obtencion del archivo
    HomePage().descargar_archivos()

    #Creamos una instancia del TRANSFORMADOR y generamos DF's
    instancia_transformador = Transformer()
    df_emae_valores = instancia_transformador.construir_df_emae_valores() #DF  de los valores del EMAE.
    df_emae_variaciones = instancia_transformador.construir_df_emae_variaciones() #DF de las variaciones del EMAE.

    #== Carga de los DF's
    #Creacion de instancia
    instancia_load = Load(host=host_dbb,user=user_dbb,password=pass_dbb,database=dbb_datalake)
    
    #En el main de la carga se manejara el envio del correo
    instancia_load.main_load(df_emae_valores,df_emae_variaciones)
    
if __name__ == '__main__':
    
    main() 