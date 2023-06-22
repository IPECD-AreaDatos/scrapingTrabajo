import datetime
import mysql.connector
import time
import numpy as np
import pandas as pd

class LoadXLS2_2:
    def loadInDataBase(self, file_path, host, user, password, database):
        # Se toma el tiempo de comienzo
        start_time = time.time()

        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        # Crear el cursor para ejecutar consultas
        cursor = conn.cursor()
        try:
            # Nombre de la tabla en MySQL
            table_name = "sipa_nacional_sin_estacionalidad"
            
            # Obtener las fechas existentes en la tabla 
            select_dates_query = "SELECT Fecha FROM sipa_nacional_sin_estacionalidad"
            cursor.execute(select_dates_query)
            existing_dates = [row[0] for row in cursor.fetchall()]


            # Leer el archivo Excel en un DataFrame de pandas
            df = pd.read_excel(file_path, sheet_name=4, skiprows=1)  # Leer el archivo XLSX y crear el DataFrame
            df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

            # Reemplazar comas por puntos en los valores numéricos
            df = df.replace(',', '.', regex=True)   
            df = df.iloc[:-9]#Elimina las ultimas 6 filas siempre
            df.drop(df.columns[-1], axis=1, inplace=True)#Elimina la ultima columna
            df = df.rename(columns=lambda x: x.strip())#Eliminar los espacion al final del nombre
            start_date = pd.to_datetime('2012-01-01')
            df['Período'] = pd.date_range(start=start_date, periods=len(df), freq='M').date
  
            for _, row in df.iterrows():
                # Obtener los valores de cada columna
                fecha = row['Período']
                empleo_privado = row['Empleo asalariado en el sector privado']
                empleo_publico = row['Empleo asalariado en el sector público']
                empleo_casas_particulares = row['Empleo en casas particulares']
                trabajo_independiente_autonomo = row['Trabajo Independientes Autónomos']
                trabajo_independiente_monotributo = row['Trabajo Independientes Monotributo']
                trabajo_independiente_monotributo_social = row['Trabajo Independientes Monotributo\nSocial']
                total = row['Total']
                if fecha in existing_dates:
                    # Actualizar los valores existentes
                    update_query = f"UPDATE {table_name} SET Empleo_Asalariado_Sector_Privado=%s, Empleo_Asalariado_Sector_Publico=%s, Empleo_Casas_Particulares=%s, Trabajo_Independiente_Automomo=%s, Trabajo_Independiente_Monotributo=%s, Trabajo_Independiente_Monotributo_Social=%s, Total=%s WHERE Fecha=%s"
                    cursor.execute(
                    update_query,
                    (empleo_privado, empleo_publico, empleo_casas_particulares, trabajo_independiente_autonomo, trabajo_independiente_monotributo, trabajo_independiente_monotributo_social, total, fecha)
                    )
                else:
                    # Insertar un nuevo registro
                    insert_query = f"INSERT INTO {table_name} (Fecha, Empleo_Asalariado_Sector_Privado, Empleo_Asalariado_Sector_Publico, Empleo_Casas_Particulares, Trabajo_Independiente_Automomo, Trabajo_Independiente_Monotributo, Trabajo_Independiente_Monotributo_Social, Total) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(
                    insert_query,
                    (fecha, empleo_privado, empleo_publico, empleo_casas_particulares, trabajo_independiente_autonomo, trabajo_independiente_monotributo, trabajo_independiente_monotributo_social, total)
                    )
                        
             # Confirmar los cambios y cerrar el cursor y la conexión
            conn.commit()
            cursor.close()
            
            # Se toma el tiempo de finalización y se c  alcula
            end_time = time.time()
            duration = end_time - start_time
            print("-----------------------------------------------")
            print("Se guardaron los datos de SIPA NACIONAL SINA ESTACIONALIDAD")
            print("Tiempo de ejecución:", duration)

            # Cerrar la conexión a la base de datos
            conn.close()

        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")
            conn.close()  # Cerrar la conexión en caso de error
            

   