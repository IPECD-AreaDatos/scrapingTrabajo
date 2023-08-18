import datetime
import time
import xlrd
import pandas as pd
import mysql.connector


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
                WHERE 
                    (ID_Categoria=1 AND ID_Division=1 AND ID_Subdivision=1) OR
                    (ID_Categoria=2 AND ID_Division=2 AND ID_Subdivision=2) OR
                    (ID_Categoria=3 AND ID_Division=5 AND ID_Subdivision=14) OR
                    (ID_Categoria=4 AND ID_Division=8 AND ID_Subdivision=17) OR
                    (ID_Categoria=5 AND ID_Division=11 AND ID_Subdivision=20) OR
                    (ID_Categoria=6 AND ID_Division=15 AND ID_Subdivision=25) OR
                    (ID_Categoria=7 AND ID_Division=17 AND ID_Subdivision=27) OR
                    (ID_Categoria=8 AND ID_Division=20 AND ID_Subdivision=30) or
                    (ID_Categoria=9 AND ID_Division=24 AND ID_Subdivision=35) OR
                    (ID_Categoria=10 AND ID_Division=26 AND ID_Subdivision=37) OR
                    (ID_Categoria=11 AND ID_Division=30 AND ID_Subdivision=41) OR
                    (ID_Categoria=12 AND ID_Division=31 AND ID_Subdivision=42) OR
                    (ID_Categoria=13 AND ID_Division=33 AND ID_Subdivision=44);
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

            # Agrupar por fecha, categoría, división y subdivisión, y sumar los valores
            df_grouped = df.groupby(['Fecha', 'ID_Categoria', 'ID_Division', 'ID_Subdivision'])['Valor'].sum().reset_index()
            
            # Insertar los valores agrupados en la tabla ipc_region con ID_Region igual a 1
            for index, row in df_grouped.iterrows():
                fecha = row['Fecha']
                id_categoria = row['ID_Categoria']
                id_division = row['ID_Division']
                id_subdivision = row['ID_Subdivision']
                valor_total = row['Valor']
                
                insert_query = """
                    INSERT INTO ipc_region (Fecha, ID_Region, ID_Categoria, ID_Division, ID_Subdivision, Valor)
                    VALUES (%s, 1, %s, %s, %s, %s)
                """
                values = (fecha, id_categoria, id_division, id_subdivision, valor_total)
                cursor.execute(insert_query, values)

            # Confirmar los cambios en la base de datos
            conn.commit()

            # Cerrar la conexión
            cursor.close()
            conn.close()
        
        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")
