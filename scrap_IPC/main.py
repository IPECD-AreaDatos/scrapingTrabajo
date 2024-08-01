from variaciones_por_region import LoadXLSDregiones
from carga_db import conexcionBaseDatos
import os
import sys
from homePage import HomePage

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
instancia_credenciales = Credenciales('datalake_economico')

print("Las credenciales son", instancia_credenciales.host,instancia_credenciales.user,instancia_credenciales.password,instancia_credenciales.database)

if __name__ == '__main__':
    #Descargar EXCEL - Tambien almacenamos las rutas que usaremos
    #home_page = HomePage()    #home_page.descargar_archivo()
    
    #↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓ CARGA DE IPC DESAGREGADO ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓
    directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
    file_path_desagregado = os.path.join(ruta_carpeta_files, 'IPC_Desagregado.xls')

    variaciones = LoadXLSDregiones(instancia_credenciales.host, instancia_credenciales.user, instancia_credenciales.password, instancia_credenciales.database).conectar_bdd()
    df = variaciones.armado_dfs(file_path_desagregado)

    conexion = conexcionBaseDatos(instancia_credenciales.host, instancia_credenciales.user, instancia_credenciales.password, instancia_credenciales.database).conectar_bdd()
    conexion.cargaBaseDatos(df)