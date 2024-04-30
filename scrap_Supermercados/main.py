#Importacion de Bibliotecas
import os
import sys
from Extraction_homePage import HomePage
from Transformation_super import Transformation_Data
from Load_super import conexionBaseDatos
from datos_deflactados import Deflactador

# Obtener la ruta al directorio actual del script
script_dir = os.path.dirname(os.path.abspath(__file__))
credenciales_dir = os.path.join(script_dir, '..', 'Credenciales_folder')
# Agregar la ruta al sys.path
sys.path.append(credenciales_dir)

# Crea una instancia de la clase "Credenciales"
from credenciales_bdd import Credenciales
instancia_credenciales = Credenciales('datalake_economico')


if __name__ == '__main__':

    #Descarga del archivo
    HomePage().descargar_archivo()

    #Obtencion del dataframe con formato solicitado
    df = Transformation_Data().contruccion_df()


    #Almacenamos los datos
    conexionBaseDatos(instancia_credenciales.host,instancia_credenciales.user,instancia_credenciales.password,instancia_credenciales.database).cargar_datos(df)

    #Deflactacion de datos
    Deflactador(instancia_credenciales.host,instancia_credenciales.user,instancia_credenciales.password,instancia_credenciales.database).main()