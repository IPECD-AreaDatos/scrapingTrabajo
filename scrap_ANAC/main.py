from home_page import HomePage
from anac_armadoDF import armadoDF
import os
from loadDatabase import load_database
from save_data_sheet import readSheets
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv()

# Variables de entorno para la base de datos
host_dbb = os.getenv('HOST_DBB')
user_dbb = os.getenv('USER_DBB')
pass_dbb = os.getenv('PASSWORD_DBB')
dbb_datalake = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

# Ruta del archivo descargado
directorio_desagregado = os.path.dirname(os.path.abspath(__file__))
ruta_carpeta_files = os.path.join(directorio_desagregado, 'files')
file_path_desagregado = os.path.join(ruta_carpeta_files, 'anac.xlsx')

if __name__ == "__main__":
    try:
        # Descarga el archivo desde la página de ANAC
        home_page = HomePage()
        home_page.descargar_archivo()

        # Armar el DataFrame a partir del archivo descargado
        df = armadoDF.armado_df(file_path_desagregado)
        if df is None:
            raise ValueError("No se pudo generar el DataFrame desde el archivo descargado.")

        # Conectar y cargar el DataFrame a la base de datos
        credenciales_datalake_economico = load_database(host_dbb, user_dbb, pass_dbb, dbb_datalake)
        credenciales_datalake_economico.conectar_bdd()

        # Corrección de valores específicos en el DataFrame
        df.loc[df["corrientes"] == 32200.000000000004, "corrientes"] = 19646
        df.loc[df["corrientes"] == 39478, "corrientes"] = 18555
        df.loc[df["corrientes"] == 40395, "corrientes"] = 19648

        # Cargar los datos en la base de datos
        credenciales_datalake_economico.load_data(df)

        # Leer los datos cargados y procesarlos
        values = credenciales_datalake_economico.read_data_excel()
        print("Valores desde la base de datos:")
        print(values)

        # Escribir los valores en Google Sheets
        readSheets().escribir_fila(values)

    except Exception as e:
        print(f"Se produjo un error durante la ejecución del script: {e}")

    finally:
        # Cerrar la conexión a la base de datos si está abierta
        if credenciales_datalake_economico:
            credenciales_datalake_economico.cerrar_conexion()
