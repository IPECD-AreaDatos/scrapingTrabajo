import datetime
import mysql.connector
import time
import xlrd

class LoadXLSDataGBA:
    def loadInDataBase(self, file_path):
        #Se toma el tiempo de comienzo
        start_time = time.time()
        
        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='Estadistica123',
            database='prueba1'
        )

        try:
            # Nombre de la tabla en MySQL
            table_name = 'ipc_regiongba'

            # Leer el archivo de xls y obtener los datos de la línea específica en la hoja 3
            workbook = xlrd.open_workbook(file_path)
            sheet = workbook.sheet_by_index(2)  # Hoja 3 (índice 2)
            target_row_index = 35  # Índice de la línea específica que deseas obtener (por ejemplo, línea 3)

            # Obtener los valores de la fila completa a partir de la segunda columna (columna B)
            target_row_values = sheet.row_values(target_row_index, start_colx=1)  # start_colx=1 indica que se inicia desde la columna B

            # Convertir los valores de número de serie de fecha de Excel a formato de fecha
            for i in range(len(target_row_values)):
                if isinstance(target_row_values[i], float):
                    excel_date = int(target_row_values[i])
                    dt = datetime.datetime(1899, 12, 30) + datetime.timedelta(days=excel_date)
                    target_row_values[i] = dt.strftime('%Y-%m-%d')

            #Datos de Nivel General
            nivel_general_values = sheet.row_values(39, start_colx=1)  # Supongamos que la nueva columna está en la fila 156
            nivel_general_values = [str(value).replace(',', '.') for value in nivel_general_values]  # Reemplazar las comas decimales por puntos decimales en los valores

            #Datos de Alimentos y bebidas no alcohólicas
            alimento_y_bebidas_no_alcoholicas_values = sheet.row_values(40, start_colx=1)  # Supongamos que la nueva columna está en la fila 156
            alimento_y_bebidas_no_alcoholicas_values = [str(value).replace(',', '.') for value in alimento_y_bebidas_no_alcoholicas_values]  # Reemplazar las comas decimales por puntos decimales en los valores
            
            #Bebidas alcohólicas y tabaco
            bebidas_alcoholicas_y_tabaco_values = sheet.row_values(41, start_colx=1)  # Supongamos que la nueva columna está en la fila 156
            bebidas_alcoholicas_y_tabaco_values = [str(value).replace(',', '.') for value in bebidas_alcoholicas_y_tabaco_values]  # Reemplazar las comas decimales por puntos decimales en los valores
            
            #Prendas de vestir y calzado
            prendasVestir_y_calzado_values = sheet.row_values(42, start_colx=1)  # Supongamos que la nueva columna está en la fila 156
            prendasVestir_y_calzado_values = [str(value).replace(',', '.') for value in prendasVestir_y_calzado_values]  # Reemplazar las comas decimales por puntos decimales en los valores
            
            #Vivienda agua electricidad gas y otros combustibles
            vivienda_agua_electricidad_gas_y_otros_combustibles_values = sheet.row_values(43, start_colx=1)  # Supongamos que la nueva columna está en la fila 156
            vivienda_agua_electricidad_gas_y_otros_combustibles_values = [str(value).replace(',', '.') for value in vivienda_agua_electricidad_gas_y_otros_combustibles_values]  # Reemplazar las comas decimales por puntos decimales en los valores
            
            #Equipamiento y mantenimiento del hogar
            equipamiento_y_mantenimiento_del_hogar_values = sheet.row_values(44, start_colx=1)  # Supongamos que la nueva columna está en la fila 156
            equipamiento_y_mantenimiento_del_hogar_values = [str(value).replace(',', '.') for value in equipamiento_y_mantenimiento_del_hogar_values]  # Reemplazar las comas decimales por puntos decimales en los valores
            
            #Salud
            salud_values = sheet.row_values(45, start_colx=1)  # Supongamos que la nueva columna está en la fila 156
            salud_values = [str(value).replace(',', '.') for value in salud_values]  # Reemplazar las comas decimales por puntos decimales en los valores
            
            #Transporte
            transporte_values = sheet.row_values(46, start_colx=1)  # Supongamos que la nueva columna está en la fila 156
            transporte_values = [str(value).replace(',', '.') for value in transporte_values]  # Reemplazar las comas decimales por puntos decimales en los valores
            
            #Comunicación
            comunicación_values = sheet.row_values(47, start_colx=1)  # Supongamos que la nueva columna está en la fila 156
            comunicación_values = [str(value).replace(',', '.') for value in comunicación_values]  # Reemplazar las comas decimales por puntos decimales en los valores
            
            #Recreación_y_cultura
            recreación_y_cultura_values = sheet.row_values(48, start_colx=1)  # Supongamos que la nueva columna está en la fila 156
            recreación_y_cultura_values = [str(value).replace(',', '.') for value in recreación_y_cultura_values]  # Reemplazar las comas decimales por puntos decimales en los valores
            
            #Educación
            educación_values = sheet.row_values(49, start_colx=1)  # Supongamos que la nueva columna está en la fila 156
            educación_values = [str(value).replace(',', '.') for value in educación_values]  # Reemplazar las comas decimales por puntos decimales en los valores
            
            #Restaurantes_y_hoteles
            restaurantes_y_hoteles_values = sheet.row_values(50, start_colx=1)  # Supongamos que la nueva columna está en la fila 156
            restaurantes_y_hoteles_values = [str(value).replace(',', '.') for value in restaurantes_y_hoteles_values]  # Reemplazar las comas decimales por puntos decimales en los valores
            
            #Bienes_y_servicios_varios
            bienes_y_servicios_varios_values = sheet.row_values(51, start_colx=1)  # Supongamos que la nueva columna está en la fila 156
            bienes_y_servicios_varios_values = [str(value).replace(',', '.') for value in bienes_y_servicios_varios_values]  # Reemplazar las comas decimales por puntos decimales en los valores
            
            # Insertar los valores en la tabla de la base de datos
            insert_query = f"INSERT INTO {table_name} (Fecha, Nivel_General, Alimentos_y_bebidas_no_alcoholicas, Bebidas_alcoholicas_y_tabaco, Prendas_de_vestir_y_calzado, Vivienda_agua_electricidad_gas_y_otros_combustibles, Equipamiento_y_mantenimiento_del_hogar, Salud, Transporte, Comunicación, Recreación_y_cultura, Educación, Restaurantes_y_hoteles, Bienes_y_servicios_varios) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

            for fecha, nivel_general, alimento_y_bebidas_no_alcoholicas, bebidas_alcoholicas_y_tabaco, prendasVestir_y_calzado, vivienda_agua_electricidad_gas_y_otros_combustibles, equipamiento_y_mantenimiento_del_hogar, salud, transporte, comunicación, recreación_y_cultura, educación, restaurantes_y_hoteles, bienes_y_servicios_varios in zip(target_row_values, nivel_general_values, alimento_y_bebidas_no_alcoholicas_values, bebidas_alcoholicas_y_tabaco_values, prendasVestir_y_calzado_values, vivienda_agua_electricidad_gas_y_otros_combustibles_values, equipamiento_y_mantenimiento_del_hogar_values, salud_values, transporte_values, comunicación_values, recreación_y_cultura_values, educación_values, restaurantes_y_hoteles_values, bienes_y_servicios_varios_values):
                conn.cursor().execute(insert_query, (fecha, float(nivel_general), float(alimento_y_bebidas_no_alcoholicas), float(bebidas_alcoholicas_y_tabaco), float(prendasVestir_y_calzado), float(vivienda_agua_electricidad_gas_y_otros_combustibles), float(equipamiento_y_mantenimiento_del_hogar), float(salud), float(transporte), float(comunicación), float(recreación_y_cultura), float(educación), float(restaurantes_y_hoteles), float(bienes_y_servicios_varios)))

            conn.commit()

            print("Se guardaron los datos de GBA")
            # Se toma el tiempo de finalización y se calcula
            end_time = time.time()
            duration = end_time - start_time
            print(f"Tiempo de ejecución: {duration} segundos")

            # Cerrar la conexión a la base de datos
            conn.close()

        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Ocurrió un error durante la carga de datos: {str(e)}")
            conn.close()  # Cerrar la conexión en caso de error