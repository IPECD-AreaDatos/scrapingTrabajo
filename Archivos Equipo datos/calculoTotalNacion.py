import mysql.connector
import time

class calculoTotalNacion:
    def loadInDataBase(self, host, user, password, database):
        # Se toma el tiempo de comienzo
        start_time = time.time()

        # Establecer la conexión a la base de datos
        conn = mysql.connector.connect(
            host=host, user=user, password=password, database=database
        )
        # Crear el cursor para ejecutar consultas
        cursor = conn.cursor()
        try:

            update_query_Nacion_Estacionalidad = f"UPDATE sipa_provincia_con_estacionalidad SET Total_Nacion = Buenos_Aires + Ciudad_Autonoma_Bs_As + Catamarca + Chaco + Chubut + Cordoba + Corrientes + Entre_Rios + Formosa + Jujuy + La_Pampa + La_Rioja + Mendoza + Misiones + Neuquen + Rio_Negro + Salta + San_Juan + San_Luis + Santa_Cruz + Santa_Fe + Santiago_del_Estero + Tierra_del_Fuego + Tucuman"
            cursor.execute(update_query_Nacion_Estacionalidad)
            update_query_NEA_Estacionalidad = f"UPDATE sipa_provincia_con_estacionalidad SET NEA = Chaco + Corrientes + Formosa + Misiones"
            cursor.execute(update_query_NEA_Estacionalidad)
            update_query_Nacion_Sin_Estacionalidad = f"UPDATE sipa_provincia_sin_estacionalidad SET Total_Nacion = Buenos_Aires + Ciudad_Autonoma_Bs_As + Catamarca + Chaco + Chubut + Cordoba + Corrientes + Entre_Rios + Formosa + Jujuy + La_Pampa + La_Rioja + Mendoza + Misiones + Neuquen + Rio_Negro + Salta + San_Juan + San_Luis + Santa_Cruz + Santa_Fe + Santiago_del_Estero + Tierra_del_Fuego + Tucuman"
            cursor.execute(update_query_Nacion_Sin_Estacionalidad)
            update_query_NEA_Sin_Estacionalidad = f"UPDATE sipa_provincia_sin_estacionalidad SET NEA = Chaco + Corrientes + Formosa + Misiones"
            cursor.execute(update_query_NEA_Sin_Estacionalidad)
            
            conn.commit()
            
            # Confirmar los cambios y cerrar el cursor y la conexión
            cursor.close()
  
            # Se toma el tiempo de finalización y se c  alcula
            end_time = time.time()
            duration = end_time - start_time
            print("-----------------------------------------------")
            print("Se calculo el total Nacion y NEA")
            print("Tiempo de ejecución:", duration)

            # Cerrar la conexión a la base de datos
            conn.close()
        except Exception as e:
            # Manejar cualquier excepción ocurrida durante la carga de datos
            print(f"Data Cuyo: Ocurrió un error durante la carga de datos: {str(e)}")
            conn.close()  # Cerrar la conexión en caso de error
            
