import datetime
import time
import xlrd
import pandas as pd
from dateutil.relativedelta import relativedelta

class LoadXLSDataProductos:
    def loadInDataBase(self, file_path_productos):
        # Se toma el tiempo de comienzo
        start_time = time.time()

        
        
        try:
            # Listas para almacenar los datos
            datos_region = []
            datos_productos = []
            fechas = []
            valores = []
            fecha_inicial = datetime.date(2017, 6, 1)
            
            workbook = xlrd.open_workbook(file_path_productos)
            
            # Seleccionar la primera hoja
            sheet = workbook.sheet_by_index(0)
            
            for fila in range(6, min(90, sheet.nrows)):
                region = sheet.cell_value(fila, 0)  # Obtener dato de la primera columna
                productos = sheet.cell_value(fila, 1)  # Obtener dato de la segunda columna
                
                # Obtener datos de la fila (excluyendo espacios en blanco)
                datos_fila = sheet.row_values(fila)[3:]
                datos_fila = [valor for valor in datos_fila if valor != ""]
                
                if datos_fila:
                    datos_region.append(region)
                    datos_productos.append(productos)
                    fechas.append(fecha_inicial.strftime('%Y-%m-%d'))  # Convertir la fecha en formato string
                    valores.append(datos_fila)
                    
                    # Incrementar la fecha para el siguiente ciclo
                    fecha_inicial = fecha_inicial + relativedelta(months=1)  # Avanzar un mes

            # Crear un DataFrame con los datos
            data = {'ID_Region': datos_region, 'Producto': datos_productos, 'Fecha': fechas, 'Valor': valores}
            df = pd.DataFrame(data)

            print(df)

            print(datos_fila)
        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")