import datetime
import time
import xlrd
import pandas as pd
import mysql.connector

class LoadXLSDataNacion:
    def loadInDataBase(self):
        # Se toma el tiempo de comienzo
        start_time = time.time()

        try:
            db_config = {
                'host': '172.17.22.10',
                'user': 'Ivan',
                'password': 'Estadistica123',
                'database': 'prueba1'
            }

            # Establecer la conexión a la base de datos MySQL
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()
            

            # Consultar y leer todos los datos de ipc_region
            select_query = """
                SELECT Fecha, ID_Region, ID_Categoria, ID_Division, ID_Subdivision, Valor
                FROM ipc_region
                WHERE NOT (ID_Categoria = 5 AND ID_Division = 13 AND ID_Subdivision = 23)
                AND NOT (ID_Categoria = 10 AND ID_Division = 27 AND ID_Subdivision = 38)
            """
            cursor.execute(select_query)
            data_to_process = cursor.fetchall()


            # Crear un DataFrame a partir de los datos obtenidos
            column_names = ['Fecha', 'ID_Region', 'ID_Categoria', 'ID_Division', 'ID_Subdivision', 'Valor']
            df = pd.DataFrame(data_to_process, columns=column_names)

            # Definir los valores de multiplicación basados en ID_Region
            df['Factor'] = df['ID_Region'].apply(lambda x: 0.447 if x == 2 else
                                                       (0.342 if x == 3 else
                                                        (0.045 if x == 4 else
                                                         (0.069 if x == 5 else
                                                          (0.052 if x == 6 else
                                                           (0.046 if x == 7 else 1.0))))))

            # Aplicar la multiplicación al valor
            df['Valor'] = df['Valor'] * df['Factor']

            # Sumar los valores agrupados por ciertas columnas
            group_columns = ['ID_Categoria', 'ID_Division', 'ID_Subdivision', 'Fecha']
            grouped_df = df.groupby(group_columns, as_index=False)['Valor'].sum()

            grouped_df_sorted = grouped_df.sort_values(by='Fecha')
        
            # Insertar el resultado en la tabla con ID_Region igual a 1
            for index, row in grouped_df_sorted.iterrows():
                fecha = row['Fecha']
                id_categoria = row['ID_Categoria']
                id_division = row['ID_Division']
                id_subdivision = row['ID_Subdivision']
                valor = row['Valor']
                
                insert_query = "INSERT INTO ipc_region (Fecha, ID_Region, ID_Categoria, ID_Division, ID_Subdivision, Valor) VALUES (%s, 1, %s, %s, %s, %s)"
                values = (fecha, id_categoria, id_division, id_subdivision, valor)
                cursor.execute(insert_query, values)
                    
            # Commit los cambios en la base de datos
            conn.commit()

            # Cerrar la conexión
            cursor.close()
            conn.close()
        
        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")