"""
Script principal para extraer estadísticas de ANAC
Descarga, procesa y carga datos de movimientos aeroportuarios
"""

import os
import logging
from datetime import datetime
from dotenv import load_dotenv
from home_page import HomePage
from anac_armadoDF import armadoDF
from loadDatabase import load_database
from save_data_sheet import readSheets

def configurar_logging():
    """Configura el sistema de logging con rotación para EC2"""
    from logging.handlers import RotatingFileHandler
    
    # Rotación de logs: máximo 5MB, mantener 2 archivos
    log_handler = RotatingFileHandler(
        'anac_scraper.log',
        maxBytes=5*1024*1024,  # 5MB
        backupCount=2
    )
    log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    
    logging.basicConfig(
        level=logging.INFO,
        handlers=[
            log_handler,
            logging.StreamHandler()
        ]
    )

def main():
    """Función principal del script"""
    configurar_logging()
    
    # Cargar variables de entorno
    load_dotenv()
    
    inicio_ejecucion = datetime.now()
    logging.info(f"=== INICIO EJECUCIÓN ANAC SCRAPER - {inicio_ejecucion} ===")
    
    # Variables de entorno para la base de datos
    host_dbb = os.getenv('HOST_DBB')
    user_dbb = os.getenv('USER_DBB')
    pass_dbb = os.getenv('PASSWORD_DBB')
    dbb_datalake = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')
    
    # Validar variables críticas
    variables_requeridas = {
        'HOST_DBB': host_dbb,
        'USER_DBB': user_dbb, 
        'PASSWORD_DBB': pass_dbb,
        'NAME_DBB_DATALAKE_ECONOMICO': dbb_datalake,
        'GOOGLE_SHEETS_API_KEY': os.getenv('GOOGLE_SHEETS_API_KEY')
    }
    
    variables_faltantes = [var for var, valor in variables_requeridas.items() if not valor]
    if variables_faltantes:
        raise ValueError(f"Variables de entorno faltantes: {', '.join(variables_faltantes)}")
    
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

        # Verificar si hay datos nuevos antes de procesar
        print("4. Verificando si hay datos nuevos...")
        if not credenciales_datalake_economico.hay_datos_nuevos(df):
            print("=== No hay datos nuevos que procesar ===")
            print("El archivo Excel no contiene fechas más recientes que la BD.")
            print("⏭ Saltando actualización de Sheets (sin datos nuevos).")
            print("=== Proceso completado - Sin cambios ===")
            return

        # Aplicar correcciones específicas a los datos
        print("5. Aplicando correcciones de datos...")
        df.loc[df["corrientes"] == 32200.000000000004, "corrientes"] = 19646
        df.loc[df["corrientes"] == 39478, "corrientes"] = 18555
        df.loc[df["corrientes"] == 40395, "corrientes"] = 19648

        # Cargar datos nuevos en la base de datos
        print("6. Cargando datos nuevos en BD...")
        credenciales_datalake_economico.conectar_bdd()
        credenciales_datalake_economico.load_data(df)

        # Leer datos para Google Sheets
        print("7. Preparando datos para Sheets...")
        ultimo_valor, ultima_fecha = credenciales_datalake_economico.read_data_excel()        

        # Actualizar Google Sheets
        if ultimo_valor is not None and ultima_fecha is not None:
            print("8. Actualizando Google Sheets...")
            readSheets().escribir_fila(ultimo_valor, ultima_fecha)
        else:
            print("❌ No se pudieron obtener datos para actualizar Sheets.")
        
        fin_ejecucion = datetime.now()
        duracion = fin_ejecucion - inicio_ejecucion
        logging.info(f"=== PROCESO COMPLETADO EXITOSAMENTE - Duración: {duracion} ===")

    except Exception as e:
        fin_ejecucion = datetime.now()
        duracion = fin_ejecucion - inicio_ejecucion
        logging.error(f"❌ ERROR DURANTE EJECUCIÓN: {e} - Duración: {duracion}")
        raise

    finally:
        # Cerrar conexión a BD
        if credenciales_datalake_economico:
            credenciales_datalake_economico.cerrar_conexion()
            logging.info("Conexión a BD cerrada.")


if __name__ == "__main__":
    main()
