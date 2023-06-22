import datetime
import mysql.connector
import time
import numpy as np
import pandas as pd


class LoadXLS5_1:
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
            table_name = "sipa_provincia_con_estacionalidad"
            
            # Obtener las fechas existentes en la tabla 
            select_dates_query = "SELECT Fecha FROM sipa_provincia_con_estacionalidad"
            cursor.execute(select_dates_query)
            existing_dates = [row[0] for row in cursor.fetchall()]


            # Leer el archivo Excel en un DataFrame de pandas
            df = pd.read_excel(file_path, sheet_name=13, skiprows=1)  # Leer el archivo XLSX y crear el DataFrame
            df = df.replace({np.nan: None})  # Reemplazar los valores NaN(Not a Number) por None

            # Reemplazar comas por puntos en los valores numéricos
            df = df.replace(',', '.', regex=True)   
            df = df.iloc[:-6]#Elimina las ultimas 6 filas siempre
            df.drop(df.columns[-1], axis=1, inplace=True)#Elimina la ultima columna
            df = df.rename(columns=lambda x: x.strip())#Eliminar los espacion al final del nombre
            start_date = pd.to_datetime('2009-01-01')
            df['Período'] = pd.date_range(start=start_date, periods=len(df), freq='M').date

            
            for _, row in df.iterrows():
                # Obtener los valores de cada columna
                fecha = row['Período']
                buenos_aires = row['BUENOS AIRES']
                ciudad_autonoma_bsas = row['Cdad. Autónoma \nde Buenos Aires']
                catamarca = row['CATAMARCA']
                chaco = row['CHACO']
                chubut = row['CHUBUT']
                cordoba = row['CÓRDOBA']
                corrientes = row['CORRIENTES']
                entre_rios = row['ENTRE RÍOS']
                formosa = row['FORMOSA']
                jujuy = row['JUJUY']
                la_pampa = row['LA PAMPA']
                la_rioja = row['LA RIOJA']
                mendoza = row['MENDOZA']
                misiones = row['MISIONES']
                neuquen = row['NEUQUÉN']
                rio_negro = row['RíO NEGRO']
                salta = row['SALTA']
                san_juan = row['SAN JUAN']
                san_luis = row['SAN LUIS']
                santa_cruz = row['SANTA CRUZ']
                santa_fe = row['SANTA FE']
                santiago_del_estero = row['SANTIAGO \nDEL ESTERO']
                tierra_del_fuego = row['TIERRA DEL FUEGO']
                tucuman = row['TUCUMÁN']
                
                if fecha in existing_dates:
                    # Actualizar los valores existentes
                    update_query = f"UPDATE {table_name} SET Buenos_Aires=%s, Ciudad_Autonoma_Bs_As=%s, Catamarca=%s, Chaco=%s, Chubut=%s, Cordoba=%s, Corrientes=%s, Entre_Rios=%s, Formosa=%s, Jujuy=%s, La_Pampa=%s, La_Rioja=%s, Mendoza=%s, Misiones=%s, Neuquen=%s, Rio_Negro=%s, Salta=%s, San_Juan=%s, San_Luis=%s, Santa_Cruz=%s, Santa_Fe=%s, Santiago_Del_Estero=%s, Tierra_Del_Fuego=%s, Tucuman=%s WHERE Fecha=%s"
                    cursor.execute(
                        update_query,
                        (
                            buenos_aires, ciudad_autonoma_bsas, catamarca, chaco, chubut, cordoba, corrientes, entre_rios, formosa, jujuy, la_pampa, la_rioja, mendoza, misiones, neuquen, rio_negro, salta, san_juan, san_luis, santa_cruz, santa_fe, santiago_del_estero, tierra_del_fuego, tucuman, fecha
                        )
                    )
                else:
                    # Insertar un nuevo registro
                    insert_query = f"INSERT INTO {table_name} (Fecha, Buenos_Aires, Ciudad_Autonoma_Bs_As, Catamarca, Chaco, Chubut, Cordoba, Corrientes, Entre_Rios, Formosa, Jujuy, La_Pampa, La_Rioja, Mendoza, Misiones, Neuquen, Rio_Negro, Salta, San_Juan, San_Luis, Santa_Cruz, Santa_Fe, Santiago_Del_Estero, Tierra_Del_Fuego, Tucuman) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    cursor.execute(
                        insert_query,
                        (
                            fecha, buenos_aires, ciudad_autonoma_bsas, catamarca, chaco, chubut, cordoba, corrientes, entre_rios, formosa, jujuy, la_pampa, la_rioja, mendoza, misiones, neuquen, rio_negro, salta, san_juan, san_luis, santa_cruz, santa_fe, santiago_del_estero, tierra_del_fuego, tucuman
                        )
                    )
            # Confirmar los cambios y cerrar el cursor y la conexión
            conn.commit()
            cursor.close()
  
            # Se toma el tiempo de finalización y se c  alcula
            end_time = time.time()
            duration = end_time - start_time
            print("-----------------------------------------------")
            print("Se guardaron los datos de SIPA PROVINCIAL CON ESTACIONALIDAD")
            print("Tiempo de ejecución:", duration)

            # Cerrar la conexión a la base de datos
            conn.close()

        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")
            conn.close()  # Cerrar la conexión en caso de error
            