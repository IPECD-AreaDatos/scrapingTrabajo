from homePage import HomePage
from loadCSVData_SP import loadCSVData_SP
from loadCSVData_Total import loadCSVData_Total
import sys
import os
import transform

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)


from credenciales_bdd import Credenciales


credenciales = Credenciales('ipecd_economico')


if __name__ == '__main__':
    #home_page = HomePage()
    #home_page.descargar_archivo()

    
    df_salario_sp = transform.datos_sp()
    df_datos_totales = transform.datos_totales()

    sys.exit()

    loadCSVData_SP().loadInDataBase(credenciales.host, credenciales.user, credenciales.password,credenciales.database)

    loadCSVData_Total().loadInDataBase(credenciales.host, credenciales.user, credenciales.password, credenciales.database)