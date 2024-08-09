import os
import sys
from conect_bdd import conexcionBaseDatos
from extract import Extraccion
from transform import Transformacion
from save_data_sheet import readSheets
from save_data_sheet import readSheets

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
credenciales = Credenciales('datalake_economico')

if __name__ == "__main__":
    print("Las credenciales son: ", credenciales.host, credenciales.user, credenciales.password, credenciales.database)
    #extraer = Extraccion()
    #extraer.descargar_archivo()
        
    # Armado del df de combustible
    df_combustible = Transformacion().crear_df()
    print("salida df")
    print(df_combustible)

    # Carga de los dfs a la base
    instancia_bdd = conexcionBaseDatos(credenciales.host, credenciales.user, credenciales.password, credenciales.database).conectar_bdd()
    # Banderas si se actualizaron las bases
    bandera = instancia_bdd.main(df_combustible)
    #Valor de bandera
    print(f"-- Condicion de carga en la base de datos: {bandera}")

    conexion_excel = readSheets(credenciales.host, credenciales.user, credenciales.password, credenciales.database).conectar_bdd()
    if bandera:
        conexion_excel.cargar_datos()
        print("Sheet actualizado.")
    else:
        print("No hay nuevos datos para el sheet.")