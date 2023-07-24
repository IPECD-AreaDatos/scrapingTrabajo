import datetime
import time
import xlrd
import pandas as pd

class LoadXLSDataCuyo:
    def loadInDataBase(self, file_path, lista_fechas ,lista_region, valor_region ,lista_subdivision, lista_valores):
        # Se toma el tiempo de comienzo
        start_time = time.time()

        try:
            

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
                    
                    

            # Agregamos NIVEL GENERAL - CODIGO: 1
            nivel_general = [cell.value for cell in sheet[9]][1:]

            for i in range(len(nivel_general)):

                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_subdivision.append(1) #--> Cargamos subdivision


            for valor in nivel_general:

                lista_valores.append(valor) #--> Cargamos valores


            #Agregamos Alimentos y bebidas no alcohólicas - Codigo 2
            alimento_y_bebidas_no_alcoholicas = [cell.value for cell in sheet[10]][1:]

            for i in range(len(alimento_y_bebidas_no_alcoholicas)):

                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_fechas.append(target_row_values[i])#--> Cargamos fechas
                lista_subdivision.append(2) #--> Cargamos subdivision



            
            for valor in alimento_y_bebidas_no_alcoholicas:

                lista_valores.append(valor)#--> Cargamos valores


    
            #Agregamos BEBIDAS ALCHOLICAS Y TABACOS - Codigo 3
            bebidas_alcoholicas_y_tabaco = list([cell.value for cell in sheet[161]][1:])

            for i in range(len(bebidas_alcoholicas_y_tabaco)):

                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_fechas.append(target_row_values[i])#--> Cargamos fechas
                lista_subdivision.append(3) #--> Cargamos subdivision



            
            for valor in bebidas_alcoholicas_y_tabaco:

                lista_valores.append(valor)#--> Cargamos valores



            #Agregamos Prendas de vestir y calzado - Codigo 4
            prendasVestir_y_calzado = [cell.value for cell in sheet[162]][1:]

            for i in range(len(prendasVestir_y_calzado)):

                lista_region.append(valor_region)#--> Cargamos region - CUYO = 6
                lista_fechas.append(target_row_values[i])#--> Cargamos fechas
                lista_subdivision.append(4) #--> Cargamos subdivision



            
            for valor in prendasVestir_y_calzado:

                lista_valores.append(valor)#--> Cargamos valores





            #Agregamos Vivienda, agua, electricidad, gas y otros combustibles - Codigo  5         
            vivienda_agua_electricidad_gas_y_otros_combustibles = list([cell.value for cell in sheet[163]][1:])

            for i in range(len(vivienda_agua_electricidad_gas_y_otros_combustibles)):

                lista_region.append(valor_region)#--> Cargamos region - CUYO = 6
                lista_fechas.append(target_row_values[i])#--> Cargamos fechas
                lista_subdivision.append(5) #--> Cargamos subdivision



            
            for valor in vivienda_agua_electricidad_gas_y_otros_combustibles:

                lista_valores.append(valor)#--> Cargamos valores


            #Agregamos Equipamiento y mantenimiento del hogar - Codigo  6         
            equipamiento_y_mantenimiento_del_hogar = list([cell.value for cell in sheet[164]][1:])

            for i in range(len(equipamiento_y_mantenimiento_del_hogar)):
                           
                lista_region.append(valor_region)#--> Cargamos region - CUYO = 6
                lista_fechas.append(target_row_values[i])#--> Cargamos fechas
                lista_subdivision.append(6) #--> Cargamos subdivision

            
            for valor in equipamiento_y_mantenimiento_del_hogar:

                lista_valores.append(valor)#--> Cargamos valores


            #Agregamos salud - Codigo  7
            
            salud = list([cell.value for cell in sheet[165]][1:])


            for i in range(len(salud )):
                           
                lista_region.append(valor_region)#--> Cargamos region - CUYO = 6
                lista_fechas.append(target_row_values[i])#--> Cargamos fechas
                lista_subdivision.append(7) #--> Cargamos subdivision

            
            for valor in salud:

                lista_valores.append(valor)#--> Cargamos valores


             #Agregamos Transporte - Codigo  8
            transporte = list([cell.value for cell in sheet[166]][1:])

            for i in range(len(transporte )):
                           
                lista_region.append(valor_region)#--> Cargamos region - CUYO = 6
                lista_fechas.append(target_row_values[i])#--> Cargamos fechas
                lista_subdivision.append(8) #--> Cargamos subdivision

            
            for valor in transporte:

                lista_valores.append(valor)#--> Cargamos valores


            

             #Agregamos Comunicacion - Codigo  9
            comunicación = list([cell.value for cell in sheet[167]][1:])


            for i in range(len(comunicación )):
                           
                lista_region.append(valor_region)#--> Cargamos region - CUYO = 6
                lista_fechas.append(target_row_values[i])#--> Cargamos fechas
                lista_subdivision.append(9) #--> Cargamos subdivision

            
            for valor in comunicación:

                lista_valores.append(valor)#--> Cargamos valores



             #Agregamos Recreación y cultura - Codigo  10
            recreación_y_cultura = list([cell.value for cell in sheet[168]][1:])


            for i in range(len(recreación_y_cultura)):
                           
                lista_region.append(valor_region)#--> Cargamos region - CUYO = 6
                lista_fechas.append(target_row_values[i])#--> Cargamos fechas
                lista_subdivision.append(10) #--> Cargamos subdivision

            
            for valor in recreación_y_cultura:

                lista_valores.append(valor)#--> Cargamos valores




            #Agregamos Educacion - Codigo  11
            educación = list([cell.value for cell in sheet[169]][1:])


            for i in range(len(educación)):
                           
                lista_region.append(valor_region)#--> Cargamos region - CUYO = 6
                lista_fechas.append(target_row_values[i])#--> Cargamos fechas
                lista_subdivision.append(11) #--> Cargamos subdivision

            
            for valor in  educación:

                lista_valores.append(valor)#--> Cargamos valores



        
            #Agregamos Restaurantes y hoteles - Codigo  12
            restaurantes_y_hoteles = list([cell.value for cell in sheet[170]][1:])

            for i in range(len(restaurantes_y_hoteles)):
                           
                lista_region.append(valor_region)#--> Cargamos region - CUYO = 6
                lista_fechas.append(target_row_values[i])#--> Cargamos fechas
                lista_subdivision.append(12) #--> Cargamos subdivision

            
            for valor in  restaurantes_y_hoteles:

                lista_valores.append(valor)#--> Cargamos valores



            #Agregamos Bienes y servicios varios - Codigo  13
            bienes_y_servicios_varios = [cell.value for cell in sheet[171]][1:]


            for i in range(len(bienes_y_servicios_varios)):
                            
                    lista_region.append(valor_region)#--> Cargamos region - CUYO = 6
                    lista_fechas.append(target_row_values[i])#--> Cargamos fechas
                    lista_subdivision.append(13) #--> Cargamos subdivision

                
            for valor in  bienes_y_servicios_varios:

                lista_valores.append(valor)#--> Cargamos valores



            

        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")