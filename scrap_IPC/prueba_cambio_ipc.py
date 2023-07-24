import datetime
import mysql.connector
import time
import xlrd
from homePage import HomePage
import os
import pandas as pd




#Datos de la base de datos
host = '172.17.22.10'
user = 'Ivan'
password = 'Estadistica123'
database = 'prueba1'


url = HomePage()
directorio_actual = os.path.dirname(os.path.abspath(__file__))
ruta_carpeta_files = os.path.join(directorio_actual, 'files')
file_path = os.path.join(ruta_carpeta_files, 'archivo.xls')


df = pd.DataFrame()



class LoadXLSDataCuyo:
    def loadInDataBase(self, file_path, host, user, password, database):
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
                    
                    
            #LLAMADA DE VAR GLOBAL 
            global df

            lista_indice = list()
            lista_valores = list()
            lista_fechas = list()
                    
            # Leer los datos de las demás filas utilizando el mismo enfoque
            nivel_general = list([cell.value for cell in sheet[159]][1:])

            for i in range(len(nivel_general)):

                lista_indice.append(1)
                lista_fechas.append(target_row_values[i])


            for valor in nivel_general:

                lista_valores.append(valor)


    

            bebidas_alcoholicas_y_tabaco = list([cell.value for cell in sheet[161]][1:])

            for i in range(len(bebidas_alcoholicas_y_tabaco)):

                lista_indice.append(2)
                lista_fechas.append(target_row_values[i])

            
            for valor in bebidas_alcoholicas_y_tabaco:

                lista_valores.append(valor)


            df['fecha'] = lista_fechas
            df['subdivision'] = lista_indice
            df['valor'] = lista_valores

            





            prendasVestir_y_calzado = [cell.value for cell in sheet[162]][1:]
            vivienda_agua_electricidad_gas_y_otros_combustibles = [cell.value for cell in sheet[163]][1:]
            equipamiento_y_mantenimiento_del_hogar = [cell.value for cell in sheet[164]][1:]
            salud = [cell.value for cell in sheet[165]][1:]
            transporte = [cell.value for cell in sheet[166]][1:]
            comunicación = [cell.value for cell in sheet[167]][1:]
            recreación_y_cultura = [cell.value for cell in sheet[168]][1:]
            educación = [cell.value for cell in sheet[169]][1:]
            restaurantes_y_hoteles = [cell.value for cell in sheet[170]][1:]
            bienes_y_servicios_varios = [cell.value for cell in sheet[171]][1:]

            

        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")

LoadXLSDataCuyo().loadInDataBase(file_path, host, user, password, database)
print(df)
