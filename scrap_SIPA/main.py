from homePage import HomePage
from loadXLSProvincias import LoadXLSProvincias
from loadXLSTrabajoRegistrado import LoadXLSTrabajoRegistrado
import os
import pandas as pd
import mysql
import datetime
import mysql.connector


#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'


if __name__ == '__main__':
    
    #Obtencion del archivo
    url = HomePage()
    directorio_actual = os.path.dirname(os.path.abspath(__file__))
    ruta_carpeta_files = os.path.join(directorio_actual, 'files')
    file_path = os.path.join(ruta_carpeta_files, 'SIPA.xlsx')

    #Inicializamos el data frame y la listas de datos
    df = pd.DataFrame() 
    lista_provincias = list()
    lista_valores_estacionalidad = list() 
    lista_valores_sin_estacionalidad = list() 
    lista_registro = list()
    lista_fechas= list() 

    #Se ejecuta el script de lectura de datos
    LoadXLSProvincias().loadInDataBase(file_path, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas)
    LoadXLSTrabajoRegistrado().loadInDataBase(file_path, lista_provincias, lista_valores_estacionalidad, lista_valores_sin_estacionalidad, lista_registro,lista_fechas)

    #Se le asigna la lista correspondiente a la columna del data frame y se arma el "Excel"
    df['fecha'] = lista_fechas
    df['id_prov'] = lista_provincias
    df['tipo_registro'] = lista_registro
    df['valores_estacionales'] = lista_valores_estacionalidad
    df['valores_no_estacionales'] = lista_valores_sin_estacionalidad
    
    #Scrip de la BASE DE DATOS
    try:
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        cursor = conn.cursor()

        # Verificar cuantas filas tiene la tabla de mysql
        select_query = "SELECT COUNT(*) FROM sipa_registro WHERE Fecha = %s"
        
        # Sentencia SQL para insertar los datos en la tabla sipa_registro
        insert_query = "INSERT INTO sipa_registro (Fecha, ID_Provincia, ID_Tipo_Registro, Cantidad_con_Estacionalidad, Cantidad_sin_Estacionalidad) VALUES (%s, %s, %s, %s, %s)"
        # Sentencia SQL para actualizar los datos en la tabla
        update_query = "UPDATE sipa_registro SET Cantidad_con_Estacionalidad = %s, Cantidad_sin_Estacionalidad = %s WHERE Fecha = %s AND ID_Provincia = %s AND ID_Tipo_Registro = %s"

        #Verificar cantidad de filas anteriores 
        select_row_count_query = "SELECT COUNT(*) FROM sipa_registro"
        cursor.execute(select_row_count_query)
        row_count_before = cursor.fetchone()[0]
        
        # Variable para controlar el estado del mensaje
        mensaje_enviado = False
        for fecha, id_prov, tipo_registro, valores_estacionales, valores_no_estacionales in zip(lista_fechas, lista_provincias, lista_registro, lista_valores_estacionalidad, lista_valores_sin_estacionalidad): #zip, itera a traves de listas paralelas
            # Convertir la fecha en formato datetime si es necesario
            if isinstance(fecha, str):
                fecha = datetime.datetime.strptime(fecha, '%Y-%m-%d').date()

            # Verificar si la fecha ya existe en la tabla
            cursor.execute(select_query, (fecha,))
            row_count = cursor.fetchone()[0]

            if row_count > 0:
                # La fecha ya existe, realizar una actualización (UPDATE)
                cursor.execute(update_query, (valores_estacionales, valores_no_estacionales, fecha, id_prov, tipo_registro))
                if not mensaje_enviado:
                    print("Se realizo una comprobacion a la base de datos")
                    mensaje_enviado = True
            else:
                # La fecha no existe, realizar una inserción (INSERT)
                cursor.execute(insert_query, (fecha, id_prov, tipo_registro, valores_estacionales, valores_no_estacionales))
                if not mensaje_enviado:
                    print("Se agregaron nuevas fechas")
                    mensaje_enviado = True
                    
        #Obtener cantidad de filas
        cursor.execute(select_row_count_query)
        row_count_after = cursor.fetchone()[0]
        #Comparar la cantidad de antes y despues
        if row_count_after > row_count_before:
            print("Se agregaron nuevos datos")
            mensaje_enviado = True
        
        # Confirmar los cambios en la base de datos
        conn.commit()
        # Cerrar el cursor y la conexión
        cursor.close()
        conn.close()

    except Exception as e:
        
        print(e)   
