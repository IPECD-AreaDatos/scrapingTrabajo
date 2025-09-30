"""
Script principal para extraer estadísticas de ANAC
Descarga, procesa y carga datos de movimientos aeroportuarios
"""

import os
from dotenv import load_dotenv
from home_page import HomePage
from anac_armadoDF import armadoDF
from loadDatabase import load_database
from save_data_sheet import readSheets

def main():
    """Función principal del script"""
    # Cargar variables de entorno
    load_dotenv()
    
    # Variables de entorno para la base de datos
    host_dbb = os.getenv('HOST_DBB')
    user_dbb = os.getenv('USER_DBB')
    pass_dbb = os.getenv('PASSWORD_DBB')
    dbb_datalake = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')
    
    # Ruta del archivo Excel procesado
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')
    file_path_excel = os.path.join(ruta_carpeta_files, 'anac.xlsx')

    
    credenciales_datalake_economico = None
    try:
        print("=== Iniciando proceso de extracción ANAC ===")
        
        # Descarga el archivo desde ANAC
        print("1. Descargando archivo...")
        home_page = HomePage()
        home_page.descargar_archivo()

        # Procesar el DataFrame del archivo descargado
        print("2. Procesando datos Excel...")
        df = armadoDF.armado_df(file_path_excel)
        if df is None:
            raise ValueError("No se pudo generar el DataFrame desde el archivo descargado.")

        # Conectar a la base de datos
        print("3. Conectando a base de datos...")
        credenciales_datalake_economico = load_database(host_dbb, user_dbb, pass_dbb, dbb_datalake)
        credenciales_datalake_economico.conectar_bdd()

        # Aplicar correcciones específicas a los datos
        print("4. Aplicando correcciones de datos...")
        df.loc[df["corrientes"] == 32200.000000000004, "corrientes"] = 19646
        df.loc[df["corrientes"] == 39478, "corrientes"] = 18555
        df.loc[df["corrientes"] == 40395, "corrientes"] = 19648

        # Cargar datos en la base de datos
        print("5. Cargando datos en BD...")
        credenciales_datalake_economico.load_data(df)

        # Leer datos para Google Sheets
        print("6. Preparando datos para Sheets...")
        values = credenciales_datalake_economico.read_data_excel()        

        # Actualizar Google Sheets
        print("7. Actualizando Google Sheets...")
        readSheets().escribir_fila(values)
        
        print("=== Proceso completado exitosamente ===")

    except Exception as e:
        print(f"❌ Error durante la ejecución: {e}")

    finally:
        # Cerrar conexión a BD
        if credenciales_datalake_economico:
            credenciales_datalake_economico.cerrar_conexion()
            print("Conexión a BD cerrada.")


if __name__ == "__main__":
    main()
