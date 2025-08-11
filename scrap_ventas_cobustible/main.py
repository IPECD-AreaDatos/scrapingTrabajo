import os
from dotenv import load_dotenv
from conect_bdd import ConexionBaseDatos
from extract import Extraccion
from transform import Transformacion
from save_data_sheet import ReadSheets

def cargar_variables_entorno():
    """Carga las variables de entorno necesarias para la conexión."""
    load_dotenv()

    # Obtener las variables de entorno
    host_dbb = os.getenv('HOST_DBB')
    user_dbb = os.getenv('USER_DBB')
    pass_dbb = os.getenv('PASSWORD_DBB')
    dbb_datalake = os.getenv('NAME_DBB_DATALAKE_ECONOMICO')

    # Verificar que todas las variables de entorno están cargadas
    if not all([host_dbb, user_dbb, pass_dbb, dbb_datalake]):
        raise ValueError("Faltan variables de entorno necesarias para la conexión.")
    
    return host_dbb, user_dbb, pass_dbb, dbb_datalake

def realizar_extraccion_datos():
    """Realiza la extracción de datos desde una fuente externa (ej. API, archivo, etc.)."""
    print("Iniciando la descarga de datos...")
    extraccion = Extraccion()
    extraccion.descargar_archivo()
    print("Datos descargados exitosamente.")

def transformar_datos():
    """Realiza la transformación de los datos y genera el DataFrame de combustible."""
    print("Transformando los datos...")
    df_combustible = Transformacion().crear_df()
    print("Transformación completa.")
    return df_combustible

def calcular_suma_por_fecha():
    """Calcula la suma de los datos para una fecha específica."""
    print("Calculando la suma de combustible para la ultima fecha...")
    suma_mensual = Transformacion().suma_por_fecha()
    print(f"Suma calculada: {suma_mensual}")
    return suma_mensual

def cargar_datos_base_datos(df_combustible, host_dbb, user_dbb, pass_dbb, dbb_datalake):
    """Carga los datos transformados a la base de datos."""
    print("Conectando a la base de datos...")
    conexion_bdd = ConexionBaseDatos(host_dbb, user_dbb, pass_dbb, dbb_datalake)
    
    # Cargar datos en la base y obtener bandera de éxito
    print("Cargando datos a la base de datos...")
    carga_exitosa = conexion_bdd.main(df_combustible)
    
    print(f"-- Condición de carga en la base de datos: {'Éxito' if carga_exitosa else 'Fracaso'}")
    
    return carga_exitosa

def actualizar_hoja_google(carga_exitosa, suma_mensual, host_dbb, user_dbb, pass_dbb, dbb_datalake):
    """Actualiza la hoja de cálculo de Google si la carga a la base de datos fue exitosa."""
    if carga_exitosa:
        print("Iniciando actualización de Google Sheets...")
        conexion_excel = ReadSheets(host_dbb, user_dbb, pass_dbb, dbb_datalake).conectar_bdd()
        conexion_excel.cargar_datos(suma_mensual)
        print("Hoja de cálculo actualizada exitosamente.")
    else:
        print(" * NO SE CONSIDERA NECESARIO ACTUALIZAR GOOGLE SHEET * ")

if __name__ == '__main__':
    try:
        # 1. Cargar las variables de entorno
        host_dbb, user_dbb, pass_dbb, dbb_datalake = cargar_variables_entorno()

        # 2. Realizar la extracción de datos
        realizar_extraccion_datos()

        # 3. Transformar los datos
        df_combustible = transformar_datos()
        print(df_combustible)
        print(df_combustible.dtypes)

        # 4. Calcular la suma para la fecha 01-09-2024
        suma_mensual = calcular_suma_por_fecha()
        # 5. Cargar los datos en la base de datos
        carga_exitosa = cargar_datos_base_datos(df_combustible, host_dbb, user_dbb, pass_dbb, dbb_datalake)

        # 6. Si la carga fue exitosa, actualizar Google Sheets
        actualizar_hoja_google(carga_exitosa, suma_mensual, host_dbb, user_dbb, pass_dbb, dbb_datalake)

    except Exception as e:
        print(f"Error en el proceso: {e}")
