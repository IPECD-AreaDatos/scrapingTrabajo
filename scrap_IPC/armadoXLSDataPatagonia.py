import datetime
import time
import xlrd
import pandas as pd

class LoadXLSDataPatagonia:
    def loadInDataBase(self, file_path, valor_region, lista_fechas, lista_region,  lista_categoria, lista_division, lista_subdivision, lista_valores):
        # Se toma el tiempo de comienzo
        start_time = time.time()

        try:
            

            # Leer el archivo de xls y obtener la hoja de trabajo específica
            workbook = xlrd.open_workbook(file_path)
            sheet = workbook.sheet_by_index(2)  # Hoja 3 (índice 2)

            # Definir el índice de la fila objetivo
            target_row_index = 252  # El índice de la fila que deseas obtener (por ejemplo, línea 3)

            # Obtener los valores de la fila completa a partir de la segunda columna (columna B)
            target_row_values = sheet.row_values(target_row_index, start_colx=1)  # start_colx=1 indica que se inicia desde la columna B

            for i in range(len(target_row_values)):
                if isinstance(target_row_values[i], float):
                    excel_date = target_row_values[i]  # Usar el valor de Excel sin convertirlo a entero
                    dt = datetime.datetime(1899, 12, 30) + datetime.timedelta(days=excel_date)
                    target_row_values[i] = dt.date()
                    
            # Leer los datos de las demás filas utilizando el mismo enfoque
            valor_sacado1 = 236.8
            valor_sacado2 = 265.8
            
            
            # Agregamos NIVEL GENERAL - CODIGO: 1
            nivel_general = [cell.value for cell in sheet[254]][1:]

            for i in range(len(nivel_general)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(1)
                lista_division.append(1)
                lista_subdivision.append(1) #--> Cargamos subdivision


            for valor in nivel_general:

                lista_valores.append(valor) #--> Cargamos valores


            #Agregamos Alimentos y bebidas no alcohólicas - Codigo 2
            alimento_y_bebidas_no_alcoholicas = [cell.value for cell in sheet[255]][1:]

            for i in range(len(alimento_y_bebidas_no_alcoholicas)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(2)
                lista_division.append(2)
                lista_subdivision.append(2) #--> Cargamos subdivision
            
            for valor in alimento_y_bebidas_no_alcoholicas:

                lista_valores.append(valor)#--> Cargamos valores


            #Agregamos Alimentos y bebidas no alcohólicas - Codigo 2
            alimento = [cell.value for cell in sheet[256]][1:]

            for i in range(len(alimento)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(2)
                lista_division.append(3)
                lista_subdivision.append(3) #--> Cargamos subdivision
            
            for valor in alimento:

                lista_valores.append(valor)#--> Cargamos valores

            #Agregamos Alimentos y bebidas no alcohólicas - Codigo 2
            pan_y_cereales = [cell.value for cell in sheet[257]][1:]

            for i in range(len(pan_y_cereales)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(2)
                lista_division.append(3)
                lista_subdivision.append(4) #--> Cargamos subdivision
            
            for valor in pan_y_cereales:

                lista_valores.append(valor)#--> Cargamos valores
                
            
            #Agregamos Alimentos y bebidas no alcohólicas - Codigo 2
            carnes_y_derivados = [cell.value for cell in sheet[258]][1:]

            for i in range(len(carnes_y_derivados)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(2)
                lista_division.append(3)
                lista_subdivision.append(5) #--> Cargamos subdivision
            
            for valor in carnes_y_derivados:

                lista_valores.append(valor)#--> Cargamos valores

            #Agregamos Alimentos y bebidas no alcohólicas - Codigo 2
            leche_productoslacteos_huevos = [cell.value for cell in sheet[259]][1:]

            for i in range(len(leche_productoslacteos_huevos)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(2)
                lista_division.append(3)
                lista_subdivision.append(6) #--> Cargamos subdivision
            
            for valor in leche_productoslacteos_huevos:

                lista_valores.append(valor)#--> Cargamos valores
            
            #Agregamos Alimentos y bebidas no alcohólicas - Codigo 2
            aceites_grasas_manteca = [cell.value for cell in sheet[260]][1:]

            for i in range(len(aceites_grasas_manteca)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(2)
                lista_division.append(3)
                lista_subdivision.append(7) #--> Cargamos subdivision
            
            for valor in aceites_grasas_manteca:

                lista_valores.append(valor)#--> Cargamos valores

            #Agregamos Alimentos y bebidas no alcohólicas - Codigo 2
            frutas = [cell.value for cell in sheet[261]][1:]

            for i in range(len(frutas)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(2)
                lista_division.append(3)
                lista_subdivision.append(8) #--> Cargamos subdivision
            
            for valor in frutas:

                lista_valores.append(valor)#--> Cargamos valores

            #Agregamos Alimentos y bebidas no alcohólicas - Codigo 2
            verduras_tuberculos_legumbres = [cell.value for cell in sheet[262]][1:]

            for i in range(len(verduras_tuberculos_legumbres)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(2)
                lista_division.append(3)
                lista_subdivision.append(9) #--> Cargamos subdivision
            
            for valor in verduras_tuberculos_legumbres:

                lista_valores.append(valor)#--> Cargamos valores

            #Agregamos Alimentos y bebidas no alcohólicas - Codigo 2
            azucar_dulces_chocolate_golosinas = [cell.value for cell in sheet[263]][1:]

            for i in range(len(azucar_dulces_chocolate_golosinas)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(2)
                lista_division.append(3)
                lista_subdivision.append(10) #--> Cargamos subdivision
            
            for valor in azucar_dulces_chocolate_golosinas:

                lista_valores.append(valor)#--> Cargamos valores
            
            #Agregamos Alimentos y bebidas no alcohólicas - Codigo 2
            bebidas_no_alcoholicas = [cell.value for cell in sheet[264]][1:]

            for i in range(len(bebidas_no_alcoholicas)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(2)
                lista_division.append(4)
                lista_subdivision.append(11) #--> Cargamos subdivision
            
            for valor in bebidas_no_alcoholicas:

                lista_valores.append(valor)#--> Cargamos valores
            
            #Agregamos Alimentos y bebidas no alcohólicas - Codigo 2
            cafe_te_yerba_cacao = [cell.value for cell in sheet[265]][1:]

            for i in range(len(cafe_te_yerba_cacao)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(2)
                lista_division.append(4)
                lista_subdivision.append(12) #--> Cargamos subdivision
            
            for valor in cafe_te_yerba_cacao:

                lista_valores.append(valor)#--> Cargamos valores
            
            #Agregamos Alimentos y bebidas no alcohólicas - Codigo 2
            aguas_minerales_bebidasgaseosas_jugos = [cell.value for cell in sheet[266]][1:]

            for i in range(len(aguas_minerales_bebidasgaseosas_jugos)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(2)
                lista_division.append(4)
                lista_subdivision.append(13) #--> Cargamos subdivision
            
            for valor in aguas_minerales_bebidasgaseosas_jugos:

                lista_valores.append(valor)#--> Cargamos valores
            
            
            #Agregamos BEBIDAS ALCHOLICAS Y TABACOS - Codigo 3
            bebidas_alcoholicas_y_tabaco = [cell.value for cell in sheet[267]][1:]

            for i in range(len(bebidas_alcoholicas_y_tabaco)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(3)
                lista_division.append(5)
                lista_subdivision.append(14) #--> Cargamos subdivision
            
            for valor in bebidas_alcoholicas_y_tabaco:

                lista_valores.append(valor)#--> Cargamos valores


            #Agregamos BEBIDAS ALCHOLICAS Y TABACOS - Codigo 3
            bebidas_alcoholicas= [cell.value for cell in sheet[268]][1:]

            for i in range(len(bebidas_alcoholicas)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(3)
                lista_division.append(6)
                lista_subdivision.append(15) #--> Cargamos subdivision
            
            for valor in bebidas_alcoholicas:

                lista_valores.append(valor)#--> Cargamos valores
            
            #Agregamos BEBIDAS ALCHOLICAS Y TABACOS - Codigo 3
            tabaco= [cell.value for cell in sheet[269]][1:]

            for i in range(len(tabaco)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(3)
                lista_division.append(7)
                lista_subdivision.append(16) #--> Cargamos subdivision
            
            for valor in tabaco:

                lista_valores.append(valor)#--> Cargamos valores
            
            
            #Agregamos Prendas de vestir y calzado - Codigo 4
            prendasVestir_y_calzado = [cell.value for cell in sheet[270]][1:]
            
            for i in range(len(prendasVestir_y_calzado)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(4)
                lista_division.append(8)
                lista_subdivision.append(17) #--> Cargamos subdivision

            for valor in prendasVestir_y_calzado:
                lista_valores.append(valor)#--> Cargamos valores

            
            #Agregamos Prendas de vestir y calzado - Codigo 4
            prendasdevestir_materiales = [cell.value for cell in sheet[271]][1:]
            
            for i in range(len(prendasdevestir_materiales)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(4)
                lista_division.append(9)
                lista_subdivision.append(18) #--> Cargamos subdivision

            for valor in prendasdevestir_materiales:
                lista_valores.append(valor)#--> Cargamos valores
                  
            # Iteramos por la lista y reemplazamos los elementos que sean '///':
            for i in range(len(lista_valores)):
                if lista_valores[i] == '///':
                    lista_valores[i] = valor_sacado1
                    
        
            #Agregamos Prendas de vestir y calzado - Codigo 4
            calzado = [cell.value for cell in sheet[272]][1:]
            
            for i in range(len(calzado)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(4)
                lista_division.append(10)
                lista_subdivision.append(19) #--> Cargamos subdivision

            for valor in calzado:
                lista_valores.append(valor)#--> Cargamos valores

            # Iteramos por la lista y reemplazamos los elementos que sean '///':
            for i in range(len(lista_valores)):
                if lista_valores[i] == '///':
                    lista_valores[i] = valor_sacado2
            
            #Agregamos Vivienda, agua, electricidad, gas y otros combustibles - Codigo  5         
            vivienda_agua_electricidad_gas_y_otros_combustibles = [cell.value for cell in sheet[273]][1:]
            
            for i in range(len(vivienda_agua_electricidad_gas_y_otros_combustibles)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(5)
                lista_division.append(11)
                lista_subdivision.append(20) #--> Cargamos subdivision

            for valor in vivienda_agua_electricidad_gas_y_otros_combustibles:

                lista_valores.append(valor)#--> Cargamos valores

            #Agregamos Vivienda, agua, electricidad, gas y otros combustibles - Codigo  5         
            alquiler_vivienda_y_gastos_conexos = [cell.value for cell in sheet[274]][1:]
            
            for i in range(len(alquiler_vivienda_y_gastos_conexos)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(5)
                lista_division.append(12)
                lista_subdivision.append(21) #--> Cargamos subdivision

            for valor in alquiler_vivienda_y_gastos_conexos:

                lista_valores.append(valor)#--> Cargamos valores

            #Agregamos Vivienda, agua, electricidad, gas y otros combustibles - Codigo  5         
            alquiler_vivienda = [cell.value for cell in sheet[275]][1:]
            
            for i in range(len(alquiler_vivienda)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(5)
                lista_division.append(12)
                lista_subdivision.append(22) #--> Cargamos subdivision

            for valor in alquiler_vivienda:

                lista_valores.append(valor)#--> Cargamos valores
        
            """
            #Agregamos Vivienda, agua, electricidad, gas y otros combustibles - Codigo  5         
            mantenimiento_y_reparacion_vivienda = [cell.value for cell in sheet[276]][1:]
            
            for i in range(len(mantenimiento_y_reparacion_vivienda)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(5)
                lista_division.append(13)
                lista_subdivision.append(23) #--> Cargamos subdivision

            for valor in mantenimiento_y_reparacion_vivienda:

                lista_valores.append(valor)#--> Cargamos valores

            """
            #Agregamos Vivienda, agua, electricidad, gas y otros combustibles - Codigo  5         
            electricidad_gas_otroscombustibles = [cell.value for cell in sheet[276]][1:]
            
            for i in range(len(electricidad_gas_otroscombustibles)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(5)
                lista_division.append(14)
                lista_subdivision.append(24) #--> Cargamos subdivision

            for valor in electricidad_gas_otroscombustibles:

                lista_valores.append(valor)#--> Cargamos valores
        
            #Agregamos Equipamiento y mantenimiento del hogar - Codigo  6         
            equipamiento_y_mantenimiento_del_hogar = [cell.value for cell in sheet[277]][1:]


            for i in range(len(equipamiento_y_mantenimiento_del_hogar)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(6)
                lista_division.append(15)
                lista_subdivision.append(25) #--> Cargamos subdivision

            
            for valor in equipamiento_y_mantenimiento_del_hogar:

                lista_valores.append(valor)#--> Cargamos valores


            #Agregamos Equipamiento y mantenimiento del hogar - Codigo  6         
            bienesyservicios_para_la_conservacion_del_hogar = [cell.value for cell in sheet[278]][1:]


            for i in range(len(bienesyservicios_para_la_conservacion_del_hogar)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(6)
                lista_division.append(16)
                lista_subdivision.append(26) #--> Cargamos subdivision

            
            for valor in bienesyservicios_para_la_conservacion_del_hogar:

                lista_valores.append(valor)#--> Cargamos valores


            #Agregamos salud - Codigo  7
            salud = [cell.value for cell in sheet[279]][1:]

            for i in range(len(salud )):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(7)
                lista_division.append(17)
                lista_subdivision.append(27) #--> Cargamos subdivision

            
            for valor in salud:
                lista_valores.append(valor)#--> Cargamos valores


            #Agregamos salud - Codigo  7
            productos_medicinales_artefactos_equiposparalasalud = [cell.value for cell in sheet[280]][1:]

            for i in range(len(productos_medicinales_artefactos_equiposparalasalud )):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(7)
                lista_division.append(18)
                lista_subdivision.append(28) #--> Cargamos subdivision

            
            for valor in productos_medicinales_artefactos_equiposparalasalud:
                lista_valores.append(valor)#--> Cargamos valores
            
            
            #Agregamos salud - Codigo  7
            gastos_prepagas = [cell.value for cell in sheet[281]][1:]

            for i in range(len(gastos_prepagas )):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(7)
                lista_division.append(19)
                lista_subdivision.append(29) #--> Cargamos subdivision

            
            for valor in gastos_prepagas:
                lista_valores.append(valor)#--> Cargamos valores
            
            
            #Agregamos Transporte - Codigo  8
            transporte = [cell.value for cell in sheet[282]][1:]

            for i in range(len(transporte)):   
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(8)
                lista_division.append(20)
                lista_subdivision.append(30) #--> Cargamos subdivision

            
            for valor in transporte:
                lista_valores.append(valor)#--> Cargamos valores


            #Agregamos Transporte - Codigo  8
            adquisicion_de_vehiculos = [cell.value for cell in sheet[283]][1:]

            for i in range(len(adquisicion_de_vehiculos)):   
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(8)
                lista_division.append(21)
                lista_subdivision.append(31) #--> Cargamos subdivision

            
            for valor in adquisicion_de_vehiculos:
                lista_valores.append(valor)#--> Cargamos valores
            
            #Agregamos Transporte - Codigo  8
            funcionamiento_equipos_transporte_personal = [cell.value for cell in sheet[284]][1:]

            for i in range(len(funcionamiento_equipos_transporte_personal)):   
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(8)
                lista_division.append(22)
                lista_subdivision.append(32) #--> Cargamos subdivision

            
            for valor in funcionamiento_equipos_transporte_personal:
                lista_valores.append(valor)#--> Cargamos valores


            #Agregamos Transporte - Codigo  8
            combustibles_lubricantes_vehiculos_hogar = [cell.value for cell in sheet[285]][1:]

            for i in range(len(combustibles_lubricantes_vehiculos_hogar)):   
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(8)
                lista_division.append(22)
                lista_subdivision.append(33) #--> Cargamos subdivision

            
            for valor in combustibles_lubricantes_vehiculos_hogar:
                lista_valores.append(valor)#--> Cargamos valores


            #Agregamos Transporte - Codigo  8
            transporte_publico = [cell.value for cell in sheet[286]][1:]

            for i in range(len(transporte_publico)):   
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(8)
                lista_division.append(23)
                lista_subdivision.append(34) #--> Cargamos subdivision

            
            for valor in transporte_publico:
                lista_valores.append(valor)#--> Cargamos valores


            #Agregamos Comunicacion - Codigo  9
            comunicación = [cell.value for cell in sheet[287]][1:]

            for i in range(len(comunicación )):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(9)
                lista_division.append(24)
                lista_subdivision.append(35) #--> Cargamos subdivision

            
            for valor in comunicación:

                lista_valores.append(valor)#--> Cargamos valores

            #Agregamos Comunicacion - Codigo  9
            servicios_telefonia_internet = [cell.value for cell in sheet[288]][1:]

            for i in range(len(servicios_telefonia_internet )):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(9)
                lista_division.append(25)
                lista_subdivision.append(36) #--> Cargamos subdivision

            
            for valor in servicios_telefonia_internet:

                lista_valores.append(valor)#--> Cargamos valores


            #Agregamos Recreación y cultura - Codigo  10
            recreación_y_cultura = [cell.value for cell in sheet[289]][1:]

            for i in range(len(recreación_y_cultura)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(10)
                lista_division.append(26)
                lista_subdivision.append(37) #--> Cargamos subdivision

            for valor in recreación_y_cultura:
                lista_valores.append(valor)#--> Cargamos valores
                """
                #Agregamos Recreación y cultura - Codigo  10
                equiposaudiovisuales_fotograficos_procesamientodelainformacion = [cell.value for cell in sheet[291]][1:]

                for i in range(len(equiposaudiovisuales_fotograficos_procesamientodelainformacion)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(10)
                lista_division.append(27)
                lista_subdivision.append(38) #--> Cargamos subdivision

                for valor in equiposaudiovisuales_fotograficos_procesamientodelainformacion:
                    lista_valores.append(valor)#--> Cargamos valores
                """
            #Agregamos Recreación y cultura - Codigo  10
            servicios_recreativos_culturales = [cell.value for cell in sheet[290]][1:]

            for i in range(len(servicios_recreativos_culturales)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(10)
                lista_division.append(28)
                lista_subdivision.append(39) #--> Cargamos subdivision

            for valor in servicios_recreativos_culturales:
                lista_valores.append(valor)#--> Cargamos valores


            #Agregamos Recreación y cultura - Codigo  10
            periodicos_diarios_revistas_libros_articulosdepapeleria = [cell.value for cell in sheet[291]][1:]

            for i in range(len(periodicos_diarios_revistas_libros_articulosdepapeleria)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(10)
                lista_division.append(29)
                lista_subdivision.append(40) #--> Cargamos subdivision

            for valor in periodicos_diarios_revistas_libros_articulosdepapeleria:
                lista_valores.append(valor)#--> Cargamos valores
                
                
            #Agregamos Educacion - Codigo  11
            educación = [cell.value for cell in sheet[292]][1:]

            for i in range(len(educación)):
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(11)
                lista_division.append(30)
                lista_subdivision.append(41) #--> Cargamos subdivision

            
            for valor in  educación:

                lista_valores.append(valor)#--> Cargamos valores



            #Agregamos Restaurantes y hoteles - Codigo  12
            restaurantes_y_hoteles = [cell.value for cell in sheet[293]][1:]

            for i in range(len(restaurantes_y_hoteles)):    
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(12)
                lista_division.append(31)
                lista_subdivision.append(42) #--> Cargamos subdivision

            
            for valor in  restaurantes_y_hoteles:
                lista_valores.append(valor)#--> Cargamos valores


            #Agregamos Restaurantes y hoteles - Codigo  12
            restaurantes_comidas_fueradelhogar = [cell.value for cell in sheet[294]][1:]
            
            for i in range(len(restaurantes_comidas_fueradelhogar)):
                lista_fechas.append(target_row_values[i])  # Cargamos fechas
                lista_region.append(valor_region)  # Cargamos region - CUYO = 6
                lista_categoria.append(12)
                lista_division.append(32)
                lista_subdivision.append(43)  # Cargamos subdivision

            for valor in  restaurantes_comidas_fueradelhogar:
                lista_valores.append(valor)#--> Cargamos valores
            
            
            #Agregamos Bienes y servicios varios - Codigo  13
            bienes_y_servicios_varios = [cell.value for cell in sheet[295]][1:]
            
            for i in range(len(bienes_y_servicios_varios)):     
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(13)
                lista_division.append(33)
                lista_subdivision.append(44) #--> Cargamos subdivision

                
            for valor in  bienes_y_servicios_varios:
                lista_valores.append(valor)#--> Cargamos valores

            #Agregamos Bienes y servicios varios - Codigo  13
            cuidado_personal = [cell.value for cell in sheet[296]][1:]
            
            for i in range(len(cuidado_personal)):     
                lista_fechas.append(target_row_values[i]) #--> Cargamos fechas
                lista_region.append(valor_region) #--> Cargamos region - CUYO = 6
                lista_categoria.append(13)
                lista_division.append(34)
                lista_subdivision.append(45) #--> Cargamos subdivision

                
            for valor in  cuidado_personal:
                lista_valores.append(valor)#--> Cargamos valores


        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")