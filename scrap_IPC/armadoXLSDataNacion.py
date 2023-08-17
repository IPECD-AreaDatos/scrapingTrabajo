import datetime
import time
import xlrd
import pandas as pd
import mysql.connector


#Datos de la base de datos
host = '127.0.0.1'
user = 'root'
password = ''
database = 'prueba1'

class LoadXLSDataNacion:
    def loadInDataBase(self, host, user, password, database ):
        # Se toma el tiempo de comienzo
        start_time = time.time()

        try:
            conn = mysql.connector.connect(
                host=host, user=user, password=password, database=database
            )
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
            factor_mapping = {
                2: 0.447,
                3: 0.342,
                4: 0.045,
                5: 0.069,
                6: 0.052,
                7: 0.046
            }

            df['Factor'] = df['ID_Region'].map(factor_mapping)

            # Aplicar la multiplicación al valor
            df['Valor'] = df['Valor'] * df['Factor']

            # Insertar los valores modificados en la tabla ipc_region con ID_Region igual a 1
            for index, row in df.iterrows():
                fecha = row['Fecha']
                id_categoria = row['ID_Categoria']
                id_division = row['ID_Division']
                id_subdivision = row['ID_Subdivision']
                valor = row['Valor']
                
                insert_query = """
                    INSERT INTO ipc_region (Fecha, ID_Region, ID_Categoria, ID_Division, ID_Subdivision, Valor)
                    VALUES (%s, 1, %s, %s, %s, %s)
                """
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

LoadXLSDataNacion().loadInDataBase(host, user, password, database)