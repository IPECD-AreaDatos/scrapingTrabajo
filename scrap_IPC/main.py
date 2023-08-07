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
lista_region = list()
lista_categoria = list()
lista_division = list()
lista_subdivision = list()
lista_valores = list()

df = pd.DataFrame()

#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'

valor_region = 2

if __name__ == '__main__':
    #Descargar EXCEL - Tambien almacenamos las rutas que usaremos
    url = HomePage()
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')
    file_path = os.path.join(ruta_carpeta_files, 'IPC_Desagregado.xls')
    valoresDeIPC = [
      #LoadXLSDataNacion,
      LoadXLSDataGBA,
      LoadXLSDataPampeana,
      LoadXLSDataNOA,
      LoadXLSDataNEA,
      LoadXLSDataCuyo,
      LoadXLSDataPatagonia,
    ]
    for regiones in valoresDeIPC:
      print("Valor region: ", valor_region)
      regiones().loadInDataBase(file_path, valor_region, lista_fechas, lista_region,  lista_categoria, lista_division, lista_subdivision, lista_valores)
      valor_region = valor_region + 1
     
     
    lista_valores[20343]=271.5
    lista_valores[20344]=275.9
    lista_valores[20345]=281.2
    lista_valores[20346]=288.8
            
    df['fecha'] = lista_fechas
    df['region'] = lista_region
    df['categoria'] = lista_categoria
    df['division']= lista_division
    df['subdivision']= lista_subdivision
    df['valor'] = lista_valores
    
    
    conn = mysql.connector.connect(
      host=host, user=user, password=password, database=database
    )
    # Crear el cursor para ejecutar consultas
    cursor = conn.cursor()
    
  
    # Sentencia SQL para comprobar si la fecha ya existe en la tabla
    select_query = "SELECT COUNT(*) FROM ipc_region WHERE Fecha = %s AND ID_Region = %s AND ID_Categoria = %s AND ID_Division = %s AND ID_Subdivision = %s"

    # Sentencia SQL para insertar los datos en la tabla
    insert_query = "INSERT INTO ipc_region (Fecha, ID_Region, ID_Categoria, ID_Division, ID_Subdivision, Valor) VALUES (%s, %s, %s, %s, %s, %s)"

    # Sentencia SQL para actualizar los datos en la tabla
    update_query = "UPDATE ipc_region SET Valor = %s WHERE Fecha = %s AND ID_Region = %s AND ID_Categoria = %s AND ID_Division = %s AND ID_Subdivision = %s"

    # Variable para controlar el estado del mensaje
    mensaje_enviado = False
    for fecha, region, categoria, division, subdivision, valor in zip(lista_fechas, lista_region, lista_categoria, lista_division, lista_subdivision, lista_valores):
      # Convertir la fecha en formato datetime si es necesario
      if isinstance(fecha, str):
          fecha = datetime.datetime.strptime(fecha, '%Y-%m-%d').date()

      # Verificar si el registro ya existe en la tabla
      cursor.execute(select_query, (fecha, region, categoria, division, subdivision))
      row_count = cursor.fetchone()[0]

      if row_count > 0:
          # El registro ya existe, realizar una actualización (UPDATE)
          cursor.execute(update_query, (valor, fecha, region, categoria, division, subdivision))
          if not mensaje_enviado:
              print("Se esta actualizo el valor de IPC: ", valor)
              print("Ningun dato nuevo")
              mensaje_enviado = True
      else:
          # El registro no existe, realizar una inserción (INSERT)
          cursor.execute(insert_query, (fecha, region, categoria, division, subdivision, valor))
          print("Se esta cargando el valor de IPC: ", valor)
          if not mensaje_enviado:
              print("Se agregaron nuevas fechas")
              mensaje_enviado = True

    # Confirmar los cambios en la base de datos
    conn.commit()

    # Cerrar el cursor y la conexión
    cursor.close()
    conn.close()



