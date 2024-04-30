import mysql
import mysql.connector
import pandas as pd
from datetime import date

class load_sipa_valores:
    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.conn = None
        self.cursor = None

    def conectar_bdd(self):
        self.conn = mysql.connector.connect(
            host=self.host, user=self.user, password=self.password, database=self.database
        )
        self.cursor = self.conn.cursor()
        return self
    
    def read_sipa(self, start_year, end_year):
        # Inicializar una lista vacía para almacenar los datos
        census_data = []
        
        # Generar una lista de fechas desde el año de inicio hasta el año de fin
        date_list = [date(year, 1, 1) for year in range(start_year, end_year + 1)]
        
        for current_date in date_list:
            # Consulta para obtener la población total de todas las provincias en la fecha actual
            query_poblacion = """
            SELECT id_provincia, SUM(cantidad_sin_estacionalidad) AS cantidad_sin_estacionalidad
            FROM datalake_economico.sipa_valores
            WHERE fecha = %s
            GROUP BY id_provincia;
            """
            self.cursor.execute(query_poblacion, (current_date.strftime('%Y-%m-%d'),))
            # Fetch all results and append them to the census_data list
            census_data.extend([
                {'id_provincia': row[0], 'cantidad_sin_estacionalidad': row[1], 'fecha': current_date}
                for row in self.cursor.fetchall()
            ])
        
        # Convertir la lista de datos en un DataFrame
        df_census = pd.DataFrame(census_data)
        print(df_census[df_census['id_provincia'] == 10])
        return df_census
