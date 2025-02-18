import os
import sys
from homePage_IPI import HomePage_IPI
from transform import Transform
from database_ipi import Database_ipi
from correo_ipi_nacion import Correo_ipi_nacion

# Cargar las variables de entorno desde el archivo .env
from dotenv import load_dotenv
load_dotenv()

host_dbb = (os.getenv('HOST_DBB'))
user_dbb = (os.getenv('USER_DBB'))
pass_dbb = (os.getenv('PASSWORD_DBB'))
dbb_datalake = (os.getenv('NAME_DBB_DATALAKE_ECONOMICO'))


if __name__ == "__main__":

    #Extract de data
    HomePage_IPI().descargar_archivo()

    #Obtencion de DF con formato adecuado
    df_valores,df_variaciones,df_var_inter_acum = Transform().main()


    #Carga en la BDD - Datalake economico
    instancia_bdd = Database_ipi(host_dbb,user_dbb,pass_dbb,dbb_datalake)
    bandera = instancia_bdd.main(df_valores,df_variaciones,df_var_inter_acum)
    #Si es V, envia correo, sino, no pasa nada.
    if bandera == True:
        instancia_correo = Correo_ipi_nacion(host_dbb,user_dbb,pass_dbb,dbb_datalake)
        instancia_correo.main()
