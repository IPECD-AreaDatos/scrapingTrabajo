from variaciones_por_region import TransformRegionesVariaciones
from valores_por_region import TransformRegionesValores
from carga_db import conexcionBaseDatos
import os
import sys
from homePage import HomePage
from datos_correo import Correo

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)
# Ahora puedes importar tus credenciales
from credenciales_bdd import Credenciales
# Después puedes crear una instancia de Credenciales
instancia_credenciales = Credenciales('datalake_economico')
instancia_credenciales2 = Credenciales('dwh_economico')

print("Las credenciales son", instancia_credenciales.host,instancia_credenciales.user,instancia_credenciales.password,instancia_credenciales.database)
print("Las credenciales2 son", instancia_credenciales2.host,instancia_credenciales2.user,instancia_credenciales2.password,instancia_credenciales2.database)

if __name__ == '__main__':

    #Descargar EXCEL - Tambien almacenamos las rutas que usaremos
    #home_page = HomePage()    
    #home_page.descargar_archivo()

    # Carga de Ipc desagregado y por categoria para Nacion
    directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
    file_path_desagregado = os.path.join(ruta_carpeta_files, 'IPC_Desagregado.xls')
    file_path_categoria = os.path.join(ruta_carpeta_files, 'IPC_categoria.xls')
    
    # Armado del df de ipc variaciones
    instancia_transform = TransformRegionesVariaciones(instancia_credenciales.host, instancia_credenciales.user, instancia_credenciales.password, instancia_credenciales.database)
    df_variaciones = instancia_transform.armado_dfs()

    # Armado del df de ipc valores
    instancia_valores = TransformRegionesValores(instancia_credenciales.host, instancia_credenciales.user, instancia_credenciales.password, instancia_credenciales.database)
    df_valores = instancia_valores.armado_dfs()

    # Carga de los dfs a la base
    instancia_bdd = conexcionBaseDatos(instancia_credenciales.host, instancia_credenciales.user, instancia_credenciales.password, instancia_credenciales.database).conectar_bdd()
    # Banderas si se actualizaron las bases
    bandera = instancia_bdd.main(df_variaciones, df_valores)

    #Valor de bandera
    print(f"-- Condicion de carga en la base de datos: {bandera}")

    sys.exit()
    correo = Correo(instancia_credenciales.host, instancia_credenciales.user, instancia_credenciales.password, instancia_credenciales.database).conectar_bdd()
    df = correo.calcular_variaciones()
    fecha_especifica = '2024-06-30'
    df_filtrado = df.loc[df['fecha'] == fecha_especifica]
    print(df_filtrado)
    exit()
    # Envío de correo si hay nuevos datos
    if bandera_var or bandera_val:
        correo = Correo(instancia_credenciales.host, instancia_credenciales.user, instancia_credenciales.password, instancia_credenciales.database).conectar_bdd()
        correo.enviar_correo()
        print("Correo enviado.")
    else:
        print("No hay nuevos datos, no se enviará ningún correo.")