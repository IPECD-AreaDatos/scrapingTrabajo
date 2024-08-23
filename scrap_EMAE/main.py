from extract import HomePage
import os
from transform import Transformer
from load import Load
import os
import sys 

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)

#Importamos biblioteca de credenciales
from credenciales_bdd import Credenciales

# Después puedes crear una instancia de Credenciales
credenciales_datalakeEconomico = Credenciales('datalake_economico')

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
    instancia_load = Load(host=credenciales_datalakeEconomico.host,user=credenciales_datalakeEconomico.user,password=credenciales_datalakeEconomico.password,
                          database=credenciales_datalakeEconomico.database)
    
    #En el main de la carga se manejara el envio del correo
    instancia_load.main_load(df_emae_valores,df_emae_variaciones)
    
if __name__ == '__main__':
    
    main() 