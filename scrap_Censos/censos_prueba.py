import openpyxl
import datetime
import os
import mysql.connector
from itertools import zip_longest

class homePage:
    def construir_df_estimaciones(self):
        directorio_actual = os.path.dirname(os.path.abspath(__file__))
        ruta_carpeta_files = os.path.join(directorio_actual, 'files')
        file_path = os.path.join(ruta_carpeta_files, 'Estimación Modificada Censo 2022.xlsx')

        # Establecer la conexión a la base de datos MySQL
        host = '172.17.22.10'
        user = 'Ivan'
        password = 'Estadistica123'
        database = 'prueba1'
        conexion = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        # Crear un cursor para ejecutar consultas
        cursor = conexion.cursor()

        # Cargar el archivo XLSX
        workbook = openpyxl.load_workbook(file_path)
        sheet = workbook.active
        
        # Obtener las fechas desde la fila 1, columna E hasta el final de la fila
        fecha_row_index = 1
        fechas_str = [cell.value for cell in sheet[fecha_row_index]][4:]

        # Convertir las fechas de tipo int a objetos de tipo date
        fechas_date = [datetime.date(year=fecha, month=1, day=1) for fecha in fechas_str]

        # Obtener los valores de ID_Provincia
        id_provincia_column_index = 1
        valores_id_provincia = [cell.value for row in sheet.iter_rows(min_row=3, min_col=id_provincia_column_index, max_col=id_provincia_column_index) for cell in row]

        # Filtrar y eliminar valores None
        valores_id_provincia = [valor for valor in valores_id_provincia if valor is not None]

        # Obtener los valores de ID_Departamento
        id_departamento_column_index = 3
        valores_id_localidades = [cell.value for row in sheet.iter_rows(min_row=3, min_col=id_departamento_column_index, max_col=id_departamento_column_index) for cell in row]

        # Filtrar y eliminar valores None
        valores_id_localidades = [valor for valor in valores_id_localidades if valor is not None]

        # Obetener valores
        id_valores_column_index = 5
        id_valores_column_index2 = 20
        valores_id_general = [cell.value for row in sheet.iter_rows(min_row=3, min_col=id_valores_column_index, max_col=id_valores_column_index2) for cell in row]

        # Filtrar y eliminar valores None
        valores_id_general = [valor for valor in valores_id_general if valor is not None]

        # Verificar la longitud de las listas
        max_len = max(len(fechas_date), len(valores_id_provincia), len(valores_id_localidades), len(valores_id_general))

        # Usar zip_longest para combinar las listas rellenando con None los valores faltantes
        zipped_lists = zip_longest(fechas_date, valores_id_provincia, valores_id_localidades, valores_id_general)

        # Sentencia SQL para comprobar si el registro ya existe en la tabla
        select_query = "SELECT COUNT(*) FROM censo_provincia WHERE Fecha = %s AND ID_Provincia = %s AND ID_Departamentos = %s"

        # Sentencia SQL para insertar los datos en la tabla censo_provincia
        insert_query = "INSERT INTO censo_provincia (Fecha, ID_Provincia, ID_Departamentos, Poblacion) VALUES (%s, %s, %s, %s)"

        # Sentencia SQL para actualizar los datos en la tabla
        update_query = "UPDATE censo_provincia SET Poblacion = %s WHERE Fecha = %s AND ID_Provincia = %s AND ID_Departamentos = %s"

        # Iterar sobre las filas y ejecutar las consultas
        for row in zipped_lists:
            fecha, id_provincia, id_localidad, poblacion = row

            # Verificar si el registro ya existe en la tabla
            cursor.execute(select_query, (fecha, id_provincia, id_localidad))
            row_count = cursor.fetchone()[0]

            if row_count > 0:
                # El registro ya existe, realizar una actualización (UPDATE)
                cursor.execute(update_query, (poblacion, fecha, id_provincia, id_localidad))
            else:
                # El registro no existe, realizar una inserción (INSERT)
                cursor.execute(insert_query, (fecha, id_provincia, id_localidad, poblacion))

        # Confirmar los cambios en la base de datos
        conexion.commit()

        # Cerrar el cursor y la conexión
        cursor.close()
        conexion.close()
            
        

homePage().construir_df_estimaciones()