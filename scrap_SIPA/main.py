from homePage import HomePage
from loadXLSProvincias import LoadXLSProvincias
from loadXLSTrabajoRegistrado import LoadXLSTrabajoRegistrado
from calculoTotalNacion import calculoTotalNacion
import os
import pandas as pd
import mysql
import datetime


#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'



#Obtencion del archivo
url = HomePage()
directorio_actual = os.path.dirname(os.path.abspath(__file__))
ruta_carpeta_files = os.path.join(directorio_actual, 'files')
file_path = os.path.join(ruta_carpeta_files, 'SIPA.xlsx')




"""
========================================================================================

                    CARGA DE PROVINCIAS

========================================================================================

"""



df = pd.DataFrame() #--> Contendra todos los datos 
lista_provincias = list() #--> Contendra los indices de la provincia
lista_valores_estacionalidad = list() #--> Contendra los valores estacionales
lista_valores_sin_estacionalidad = list() #--> Contendra los valores no estacionales
lista_registro = list()#--> Contendra el tipo de REGISTRO
lista_fechas= list() #--> Contendra las fechas


LoadXLSProvincias().loadInDataBase(file_path, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas)
LoadXLSTrabajoRegistrado().loadInDataBase(file_path, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas)

df['fecha'] = lista_fechas
df['id_prov'] = lista_provincias
df['tipo_registro'] = lista_registro
df['valores_estacionales'] = lista_valores_estacionalidad
df['valores_no_estacionales'] = lista_valores_sin_estacionalidad


try:
    conn = mysql.connector.connect(
        host=host, user=user, password=password, database=database
    )
    cursor = conn.cursor()

    # Sentencia SQL para comprobar si la fecha ya existe en la tabla
    select_query = "SELECT COUNT(*) FROM sipa_registro WHERE Fecha = %s"
    
    # Sentencia SQL para insertar los datos en la tabla sipa_registro
    insert_query = "INSERT INTO sipa_registro (Fecha, ID_Provincia, ID_Tipo_Registro, Cantidad_con_Estacionalidad, Cantidad_sin_Estacionalidad) VALUES (%s, %s, %s, %s, %s)"
    # Sentencia SQL para actualizar los datos en la tabla
    update_query = "UPDATE sipa_registro SET ID_Provincia = %s WHERE Fecha = %s AND ID_Tipo_Registro = %s AND Cantidad_con_Estacionalidad = %s AND Cantidad_sin_Estacionalidad= %s"

    # Variable para controlar el estado del mensaje
    mensaje_enviado = False
    for fecha, id_prov, tipo_registro, valores_estacionales, valores_no_estacionales in zip(lista_fechas, lista_provincias, lista_registro, lista_valores_estacionalidad, lista_valores_sin_estacionalidad):
        # Convertir la fecha en formato datetime si es necesario
        if isinstance(fecha, str):
            fecha = datetime.datetime.strptime(fecha, '%Y-%m-%d').date()

        # Verificar si la fecha ya existe en la tabla
        cursor.execute(select_query, (fecha,))
        row_count = cursor.fetchone()[0]

        if row_count > 0:
            # La fecha ya existe, realizar una actualización (UPDATE)
            cursor.execute(update_query, (id_prov, fecha, tipo_registro, valores_estacionales, valores_no_estacionales))
            if not mensaje_enviado:
                print("Ningun dato nuevo")
                mensaje_enviado = True
        else:
            # La fecha no existe, realizar una inserción (INSERT)
            cursor.execute(insert_query, (fecha, id_prov, tipo_registro, valores_estacionales, valores_no_estacionales))
            if not mensaje_enviado:
                print("Se agregaron nuevas fechas")
                mensaje_enviado = True

    # Confirmar los cambios en la base de datos
    conn.commit()
    # Cerrar el cursor y la conexión
    cursor.close()
    conn.close()

except Exception as e:
    
   print(e)   

try:

   # Cerrar el cursor y la conexión
   if cursor is not None:
      cursor.close()
   if conn is not None:
      conn.close()

except Exception as e:
    
    print(e)