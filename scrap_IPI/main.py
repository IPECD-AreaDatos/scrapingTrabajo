import os
import sys
from homePage_IPI import HomePage_IPI
from transform import Transform
from database_ipi import Database_ipi
from correo_ipi_nacion import Correo_ipi_nacion

#Configuracion de la ruta de credenciales
# Obtiene la ruta absoluta al directorio donde reside el script actual.
script_dir = os.path.dirname(os.path.abspath(__file__))
# Crea una ruta al directorio 'Credenciales_folder' que se supone está un nivel arriba en la jerarquía de directorios.
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)

# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales = Credenciales('ipecd_economico')
credenciales2 = Credenciales('datalake_economico')


if __name__ == "__main__":

    #Extract de data
    #HomePage_IPI().descargar_archivo()

    #Obtencion de DF con formato adecuado
    #df_valores,df_variaciones,df_var_inter_acum = Transform().main()


    #Carga en la BDD - Datalake economico
    #instancia_bdd = Database_ipi(credenciales2.host, credenciales2.user, credenciales2.password, credenciales2.database)
    #bandera = instancia_bdd.main(df_valores,df_variaciones,df_var_inter_acum)
    
    bandera = False
    #Si es V, envia correo, sino, no pasa nada.
    if bandera == True:
        instancia_correo = Correo_ipi_nacion(credenciales2.host, credenciales2.user, credenciales2.password, credenciales2.database)
        instancia_correo.main()
