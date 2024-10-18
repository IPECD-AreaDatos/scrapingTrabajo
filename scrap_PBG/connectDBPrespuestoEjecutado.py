import mysql.connector
import pandas as pd

class connectDBPrespuestoEjecutado:
    def __init__(self, host, user, password, database):
        """
        Inicializa la conexión a la base de datos con las credenciales proporcionadas.
        """
        self.connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()

    def __del__(self):
        """
        Asegura que el cursor y la conexión se cierren cuando la instancia se destruye.
        """
        self.cursor.close()
        self.connection.close()

    def update_database_with_new_data(self, df):

        table_name= 'pbg_presupuesto_ejecutado_uno_ivan'
        for index, row in df.iterrows():
                # Luego, puedes usar estos valores en tu consulta SQL
                sql_insert = f"INSERT INTO {table_name} (mes, año, jurisdiccion, gastos_en_personal, bienes_de_consumo, servicios_no_personales, bienes_de_uso) VALUES (%s, %s, %s, %s, %s, %s, %s)"
                # Ejecutar la sentencia SQL de inserción
                self.cursor.execute(sql_insert, (row['mes'], row['año'], row['jurisdiccion'], row['gastos_en_personal'], row['bienes_de_consumo'], row['servicios_no_personales'], row['bienes_de_uso']))
        self.connection.commit()
        print("Se cargo al base de datos")





