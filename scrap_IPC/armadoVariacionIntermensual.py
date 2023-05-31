import mysql.connector
import time

class armadoVariacionIntermensual:
    def calculoVariacion(self):
        #Se toma el tiempo de comienzo
        start_time = time.time()
        
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='Estadistica123',
            database='prueba1'
        )
        
        # Definir el nombre de la tabla original y la tabla de resultados
        tabla_original = 'ipc_totalnacion'
        tabla_resultados = 'variacion_interanual_nacion'

        # Obtener los datos de la tabla original
        select_query = f"SELECT * FROM {tabla_original}"
        cursor = conn.cursor()
        cursor.execute(select_query)
        rows = cursor.fetchall()

       # Calcular la diferencia porcentual entre filas consecutivas a partir de la tercera fila y guardar los resultados en una lista
        resultados = []
        for i in range(2, len(rows)):
            fila_actual = rows[i]
            fila_anterior = rows[i - 1]
            diferencia_porcentual = []
            for j in range(1, len(fila_actual)):
                diferencia_porcentual.append((fila_actual[j] / fila_anterior[j]) - 1)  # Calcular la diferencia porcentual para cada columna
            resultados.append(diferencia_porcentual)

        # Insertar los resultados en la tabla existente
        insert_query = f"INSERT INTO {tabla_resultados} (Fecha"
        for i in range(1, len(rows[0])):
            insert_query += f", columna_{i}"  # Cambia 'columna' por el nombre correspondiente a cada columna en tu tabla
        insert_query += ") VALUES (%s"
        for i in range(1, len(rows[0])):
            insert_query += ", %s"
        insert_query += ")"

        for i in range(1, len(rows)):
            fila = rows[i]
            cursor.execute(insert_query, tuple(fila[1:]))

        # Confirmar los cambios y cerrar la conexión
        conn.commit()
        conn.close()

        for i in range(1, len(rows)):
            fila = rows[i]
            cursor.execute(insert_query, tuple(fila))

        # Confirmar los cambios y cerrar la conexión
        conn.commit()
        conn.close()
        
        # Calcular y mostrar el tiempo de ejecución
        end_time = time.time()
        execution_time = end_time - start_time
        print("Tiempo de ejecución:", execution_time)
