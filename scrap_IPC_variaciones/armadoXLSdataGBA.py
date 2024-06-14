import datetime
import mysql.connector
import time
import xlrd


class LoadXLSDataGBA:
    def loadInDataBase(self, file_path, valor_region, lista_fechas, lista_region,  lista_categoria, lista_division, lista_subdivision, lista_valores):

            
        # Leer el archivo de xls y obtener la hoja de trabajo específica
        workbook = xlrd.open_workbook(file_path)
        sheet = workbook.sheet_by_index(0)  # Hoja 3 (índice 2)

        target_value="Región GBA"
        
        filasFecha= buscar_fila_por_valor(sheet, target_value)
        
        print("Numero de fila", filasFecha)
        
        # Definir el índice de la fila objetivo
        target_row_index = filasFecha # El índice de la fila que deseas obtener (por ejemplo, línea 3)

        # Obtener los valores de la fila completa a partir de la segunda columna (columna B)
        target_row_values = sheet.row_values(target_row_index + 3, start_colx=1)  # start_colx=1 indica que se inicia desde la columna B

        print(target_row_values)

def buscar_fila_por_valor(sheet, target_value):
    # Iterar sobre las filas de la hoja de trabajo
    for i in range(sheet.nrows):
        # Iterar sobre las celdas de la fila actual
        for j in range(sheet.ncols):
            # Obtener el valor de la celda actual
            cell_value = sheet.cell_value(i, j)
            
            # Verificar si el valor objetivo está presente en la celda
            if target_value == cell_value:
                return i # Devolver el número de fila (se suma 1 porque los índices comienzan desde 0)

    # Si no se encuentra el valor en ninguna celda, devolver None
    return None
