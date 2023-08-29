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
            region = []
            producto = []
            fecha = []
            valor = []
            fecha_inicial = datetime.date(2017, 6, 1)

            # Abrir el archivo de Excel
            workbook = xlrd.open_workbook(file_path_productos)

            # Seleccionar la primera hoja
            sheet = workbook.sheet_by_index(0)

            # Leer los datos de las filas
            for fila in range(6, min(90, sheet.nrows)):
                # Obtener la región
                region.append(sheet.cell_value(fila, 0))

                # Obtener el producto
                producto.append(sheet.cell_value(fila, 1))

                # Obtener los datos de la fila
                datos_fila = sheet.row_values(fila)[3:]
                datos_fila = [valor for valor in datos_fila if valor != ""]

                # Agregar los datos a las listas
                if datos_fila:
                    fecha.append(fecha_inicial.strftime('%Y-%m-%d'))
                    valor.append(datos_fila)

                    # Actualizar la fecha para el siguiente ciclo
                    fecha_inicial = fecha_inicial + relativedelta(months=1)

            # Crear un diccionario con los datos
            data = {
                'ID_Region': region,
                'Fecha': fecha,
                'Producto': producto,
                'Valor': valor,
            }

            # Crear un DataFrame con los datos
            df = pd.DataFrame(data)

            # Ajustar los tipos de datos
            df['ID_Region'] = df['ID_Region'].astype(int)
            df['Fecha'] = pd.to_datetime(df['Fecha'])
            df['Valor'] = df['Valor'].astype(float)

            # Transformar las regiones a ID
            regiones = {
                "Nacion": 1,
                "GBA": 2,
                "Pampeana": 3,
                "NOA": 4,
                "NEA": 5,
                "Cuyo": 6,
                "Patagonia": 7,
            }

            for i in range(len(df)):
                region = df.loc[i, 'ID_Region']
                for palabra in ["Nación", "GBA", "Pampeana", "NOA", "NEA", "Cuyo", "Patagonia"]:
                    if region == palabra:
                        df.loc[i, 'ID_Region'] = regiones[palabra]

            # Guardar el DataFrame en un archivo CSV
            df.to_csv('ipc_productos.csv', index=False)
            
        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")