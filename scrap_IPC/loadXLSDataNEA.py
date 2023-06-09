import datetime
import mysql.connector
import time
import xlrd

class LoadXLSDataNEA:
    def loadInDataBase(self, file_path, host, user, password, database):
        #Se toma el tiempo de comienzo
        start_time = time.time()
        
        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        # Crear el cursor para ejecutar consultas
        cursor = conn.cursor()
        try:
            # Nombre de la tabla en MySQL
            table_name = "ipc_regionnea"

            # Obtener las fechas existentes en la tabla ipc_regionnea
            select_dates_query = "SELECT Fecha FROM ipc_regionnea"
            cursor.execute(select_dates_query)
            existing_dates = [row[0] for row in cursor.fetchall()]

            # Leer el archivo de xls y obtener la hoja de trabajo específica
            workbook = xlrd.open_workbook(file_path)
            sheet = workbook.sheet_by_index(2)  # Hoja 3 (índice 2)

            # Definir el índice de la fila objetivo
            target_row_index = 155  # El índice de la fila que deseas obtener (por ejemplo, línea 3)

            # Obtener los valores de la fila completa a partir de la segunda columna (columna B)
            target_row_values = sheet.row_values(target_row_index, start_colx=1)  # start_colx=1 indica que se inicia desde la columna B

            for i in range(len(target_row_values)):
                if isinstance(target_row_values[i], float):
                    excel_date = target_row_values[i]  # Usar el valor de Excel sin convertirlo a entero
                    dt = datetime.datetime(1899, 12, 30) + datetime.timedelta(days=excel_date)
                    target_row_values[i] = dt.date()
                    
            # Leer los datos de las demás filas utilizando el mismo enfoque
            nivel_general = [cell.value for cell in sheet[129]][1:]
            alimento_y_bebidas_no_alcoholicas = [cell.value for cell in sheet[130]][1:]
            bebidas_alcoholicas_y_tabaco = [cell.value for cell in sheet[131]][1:]
            prendasVestir_y_calzado = [cell.value for cell in sheet[132]][1:]
            vivienda_agua_electricidad_gas_y_otros_combustibles = [cell.value for cell in sheet[133]][1:]
            equipamiento_y_mantenimiento_del_hogar = [cell.value for cell in sheet[134]][1:]
            salud = [cell.value for cell in sheet[135]][1:]
            transporte = [cell.value for cell in sheet[136]][1:]
            comunicación = [cell.value for cell in sheet[137]][1:]
            recreación_y_cultura = [cell.value for cell in sheet[138]][1:]
            educación = [cell.value for cell in sheet[139]][1:]
            restaurantes_y_hoteles = [cell.value for cell in sheet[140]][1:]
            bienes_y_servicios_varios = [cell.value for cell in sheet[141]][1:]

            for (
                fecha,
                nivel_general,
                alimento_y_bebidas_no_alcoholicas,
                bebidas_alcoholicas_y_tabaco,
                prendasVestir_y_calzado,
                vivienda_agua_electricidad_gas_y_otros_combustibles,
                equipamiento_y_mantenimiento_del_hogar,
                salud,
                transporte,
                comunicación,
                recreación_y_cultura,
                educación,
                restaurantes_y_hoteles,
                bienes_y_servicios_varios,
            ) in zip(
                target_row_values,
                nivel_general,
                alimento_y_bebidas_no_alcoholicas,
                bebidas_alcoholicas_y_tabaco,
                prendasVestir_y_calzado,
                vivienda_agua_electricidad_gas_y_otros_combustibles,
                equipamiento_y_mantenimiento_del_hogar,
                salud,
                transporte,
                comunicación,
                recreación_y_cultura,
                educación,
                restaurantes_y_hoteles,
                bienes_y_servicios_varios,
            ):
                if fecha not in existing_dates:
                    print("fecha---->", fecha, "Existente----->", existing_dates)
                    insert_query = f"INSERT INTO {table_name} (Fecha, Nivel_General, Alimentos_y_bebidas_no_alcoholicas, Bebidas_alcoholicas_y_tabaco, Prendas_de_vestir_y_calzado, Vivienda_agua_electricidad_gas_y_otros_combustibles, Equipamiento_y_mantenimiento_del_hogar, Salud, Transporte, Comunicación, Recreación_y_cultura, Educación, Restaurantes_y_hoteles, Bienes_y_servicios_varios) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(
                        insert_query,
                        (
                            fecha,
                            float(nivel_general),
                            float(alimento_y_bebidas_no_alcoholicas),
                            float(bebidas_alcoholicas_y_tabaco),
                            float(prendasVestir_y_calzado),
                            float(vivienda_agua_electricidad_gas_y_otros_combustibles),
                            float(equipamiento_y_mantenimiento_del_hogar),
                            float(salud),
                            float(transporte),
                            float(comunicación),
                            float(recreación_y_cultura),
                            float(educación),
                            float(restaurantes_y_hoteles),
                            float(bienes_y_servicios_varios),
                        ),
                    )
                else:
                    # Actualizar los valores de variación en la fila existente
                    update_query = "UPDATE ipc_regionnea SET Nivel_General = %s, Alimentos_y_bebidas_no_alcoholicas = %s, Bebidas_alcoholicas_y_tabaco = %s, Prendas_de_vestir_y_calzado = %s, Vivienda_agua_electricidad_gas_y_otros_combustibles = %s, Equipamiento_y_mantenimiento_del_hogar = %s, Salud = %s, Transporte = %s, Comunicación = %s, Recreación_y_cultura = %s, Educación = %s, Restaurantes_y_hoteles = %s, Bienes_y_servicios_varios = %s WHERE Fecha = %s"
                    update_values = (
                        float(nivel_general),
                        float(alimento_y_bebidas_no_alcoholicas),
                        float(bebidas_alcoholicas_y_tabaco),
                        float(prendasVestir_y_calzado),
                        float(vivienda_agua_electricidad_gas_y_otros_combustibles),
                        float(equipamiento_y_mantenimiento_del_hogar),
                        float(salud),
                        float(transporte),
                        float(comunicación),
                        float(recreación_y_cultura),
                        float(educación),
                        float(restaurantes_y_hoteles),
                        float(bienes_y_servicios_varios),
                        fecha,
                    )
                    cursor.execute(update_query, update_values)

            # Confirmar los cambios en la base de datos
            conn.commit()

            print("Se guardaron los datos del NEA")
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