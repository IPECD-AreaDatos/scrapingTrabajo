from homePage import HomePage
from loadCSVData_SP import Gestion_bdd
import sys
import os
import transform

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)


from credenciales_bdd import Credenciales


credenciales = Credenciales('datalake_economico')

if __name__ == '__main__':
    home_page = HomePage()
    home_page.descargar_archivo()

    #Obtencion de los datos con el formato correcto 
    df_salario_sp = transform.datos_sp() #--> Datos del PRIVADO
    df_datos_totales = transform.datos_totales() #--> Datos TOTALES
    
    #Creamos instancia para cargar datos en el DATALAKE
    instancia = Gestion_bdd(host=credenciales.host, user=credenciales.user, password=credenciales.password, database=credenciales.database)

    #Carga de DATALAKE de datos de Salario Privado
    instancia.loadInDataBase(df_salario_sp,'dp_salarios_sector_privado')
    instancia.loadInDataBase(df_datos_totales,'dp_salarios_total')



