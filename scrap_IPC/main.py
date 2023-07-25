from homePage import HomePage
from armadoXLSDataNacion import LoadXLSDataNacion
from armadoXLSDataGBA import LoadXLSDataGBA
from armadoXLSDataPampeana import LoadXLSDataPampeana
from armadoXLSDataNOA import LoadXLSDataNOA
from armadoXLSDataNEA import LoadXLSDataNEA
from armadoXLSDataCuyo import LoadXLSDataCuyo
from armadoXLSDataPatagonia import LoadXLSDataPatagonia
import os
import pandas as pd
import mysql.connector
import datetime

#Listas a tratar durante el proceso
lista_fechas = list()
lista_regiones = list()
lista_subdivision = list()
lista_valores = list()

df = pd.DataFrame()

#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

valor_region = 1

if __name__ == '__main__':
    #Descargar EXCEL - Tambien almacenamos las rutas que usaremos
    url = HomePage()
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')
    file_path = os.path.join(ruta_carpeta_files, 'archivo.xls')
    valoresDeIPC = [
      LoadXLSDataNacion,
      LoadXLSDataGBA,
      LoadXLSDataPampeana,
      LoadXLSDataNOA,
      LoadXLSDataNEA,
      LoadXLSDataCuyo,
      LoadXLSDataPatagonia,
    ]
    for regiones in valoresDeIPC:
      print("Valor region: ", valor_region)
      regiones().loadInDataBase(file_path, lista_fechas, lista_regiones, valor_region ,lista_subdivision, lista_valores)
      valor_region = valor_region + 1
       
    df['fecha'] = lista_fechas
    df['regiones'] = lista_regiones
    df['subdivision'] = lista_subdivision
    df['valores'] = lista_valores
    
    conn = mysql.connector.connect(
      host=host, user=user, password=password, database=database
    )
    # Crear el cursor para ejecutar consultas
    cursor = conn.cursor()
    

    
    # Sentencia SQL para comprobar si la fecha ya existe en la tabla
    select_query = "SELECT COUNT(*) FROM ipc_region WHERE Fecha = %s"

    # Sentencia SQL para insertar los datos en la tabla
    insert_query = "INSERT INTO ipc_region (Fecha, ID_Region, ID_Subdivision, Valor) VALUES (%s, %s, %s, %s)"

    # Sentencia SQL para actualizar los datos en la tabla
    update_query = "UPDATE ipc_region SET Valor = %s WHERE Fecha = %s AND ID_Region = %s AND ID_Subdivision = %s"

    # Variable para controlar el estado del mensaje
    mensaje_enviado = False
    for fecha, region, subdivision, valor in zip(lista_fechas, lista_regiones, lista_subdivision, lista_valores):
        # Convertir la fecha en formato datetime si es necesario
        if isinstance(fecha, str):
            fecha = datetime.datetime.strptime(fecha, '%Y-%m-%d').date()

        # Verificar si la fecha ya existe en la tabla
        cursor.execute(select_query, (fecha,))
        row_count = cursor.fetchone()[0]

        if row_count > 0:
            # La fecha ya existe, realizar una actualización (UPDATE)
            cursor.execute(update_query, (valor, fecha, region, subdivision))
            if not mensaje_enviado:
                print("Ningun dato nuevo")
                mensaje_enviado = True
        else:
            # La fecha no existe, realizar una inserción (INSERT)
            cursor.execute(insert_query, (fecha, region, subdivision, valor))
            if not mensaje_enviado:
                print("Se agregaron nuevas fechas")
                mensaje_enviado = True

    # Confirmar los cambios en la base de datos
    conn.commit()

    # Cerrar el cursor y la conexión
    cursor.close()
    conn.close()



