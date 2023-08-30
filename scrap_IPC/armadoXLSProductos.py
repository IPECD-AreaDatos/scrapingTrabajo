import datetime
import time
import xlrd
import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
import pymysql

class LoadXLSDataProductos:
    def loadInDataBase(self, file_path_productos):
        # Se toma el tiempo de comienzo
        start_time = time.time()
        
        try:
            # Listas para almacenar los datos
            id_region = []
            productos = []
            fechas = []
            valores = []
            nuevo_valor = [278.39, 62.24, 117.71, 112.76, 50.13, 46.89]
            indice_nuevo_valor = 0 
            fecha_inicial = datetime.date(2017, 6, 1)

            # Abrir el archivo de Excel
            workbook = xlrd.open_workbook(file_path_productos)

            # Seleccionar la primera hoja
            sheet = workbook.sheet_by_index(0)

            # Variables para el seguimiento del producto y la fecha actual
            producto_anterior = None
            fecha_actual = fecha_inicial
                
            # Iterar a través de las filas hasta la fila 90
            for fila in range(6, min(90, sheet.nrows)):
                region = sheet.cell_value(fila, 0)  # Obtener dato de la primera columna
                producto = sheet.cell_value(fila, 1)  # Obtener dato de la segunda columna

                # Obtener datos de la fila (excluyendo espacios en blanco)
                datos_fila = sheet.row_values(fila)[3:]
                datos_fila = [valor for valor in datos_fila if valor != ""]

                if datos_fila:
                    if producto != producto_anterior:
                        producto_anterior = producto
                        fecha_actual = fecha_inicial
                    
                    for valor in datos_fila:
                        if valor == '///':
                            valores.append(nuevo_valor[indice_nuevo_valor])
                            indice_nuevo_valor += 1
                        else:
                            valores.append(valor)
                        
                        id_region.append(region)  # Agregar región para este valor
                        productos.append(producto)  # Agregar producto para este valor
                        fechas.append(fecha_actual)  # Agregar fecha actual para este valor

                        # Avanzar la fecha por 1 mes para el siguiente ciclo
                        fecha_actual = fecha_actual + relativedelta(months=1)
                    


            for indice, valor in enumerate(valores):
                if valor == '///':
                    valores[indice] = nuevo_valor[indice_nuevo_valor]
                    indice_nuevo_valor += 1  # Incrementar el índice de nuevo_valor
                else:
                    valores[indice] = valor

        
            # Crear un DataFrame con los datos
            data = {
                'ID_Region': id_region,
                'Fecha': fechas,
                'Producto': productos,
                'Valor': valores,
            }

            df = pd.DataFrame(data)

            # Transformar las regiones a ID
            regiones = {
                "Nacion": 1,
                "GBA": 2,
                "Pampeana": 3,
                "Noreste": 4,
                "Noroeste": 5,
                "Cuyo": 6,
                "Patagonia": 7,
            }

            df['ID_Region'] = df['ID_Region'].map(regiones)

            # Ajustar los tipos de datos
            df['Fecha'] = pd.to_datetime(df['Fecha'])
            df = df.astype({'ID_Region': int, 'Valor': float})

            print(df)
            
            # Configuración de la conexión a la base de datos
            db_config = {
                'user': 'Ivan',
                'password': 'Estadistica123',
                'host': '172.17.22.10',
                'database': 'prueba1',
            }

            # Crear una conexión a la base de datos MySQL
            engine = create_engine(f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}")
            connection = pymysql.connect(host=db_config['host'], user=db_config['user'], password=db_config['password'], database=db_config['database'])

            # Nombre de la tabla en MySQL
            table_name = 'ipc_productos'

            # Consulta para obtener la cantidad de registros antes del truncado
            row_count_before = f"SELECT COUNT(*) FROM {table_name}"
            
            # Truncar la tabla en MySQL antes de la carga
            truncate_query = f"TRUNCATE TABLE {table_name}"
            
            with connection.cursor() as cursor:
                cursor.execute(truncate_query)

            # ... Código para cargar los datos en el DataFrame ...

            # Extraer solo la fecha de la columna 'Fecha'
            df['Fecha'] = df['Fecha'].dt.date
            
            # Subir el DataFrame a la tabla ipc_productos en MySQL
            df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

            # Consulta para obtener la cantidad de registros después de la carga
            row_count_after = f"SELECT COUNT(*) FROM {table_name}"

            #Comparar la cantidad de antes y despues
            if row_count_after > row_count_before:
                print("Se agregaron nuevos datos de ipc_productos")
            else:
                print("Se realizo una verificacion de la base de datos")
                   
        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")